# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import os
import json
import shutil
import functools
from pickle import dump, load
import importlib
import duckdb
import numpy as np
import pandas as pd
from sklearn import metrics
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import FunctionTransformer
from sklearn.pipeline import Pipeline

sandbox_path = 'sandbox/'
features_path = 'feature_generation/'
model_path = 'model/'
db_file = 'database.db'
extension = ".pickle"


def load_data(info):
    with duckdb.connect(database=features_path + db_file, read_only=True) as con:
        train = con.execute(f"SELECT * FROM {info['analysis']}.{info['data']}_train").df()
        test = con.execute(f"SELECT * FROM {info['analysis']}.{info['data']}_test").df()
    return train, test


def get_object(module_path):
    module_path = module_path.split('.')
    module_name, object_name = '.'.join(module_path[:-1]), module_path[-1]
    module = importlib.import_module(module_name)
    return getattr(module, object_name)


def generate_model(info, model_parameters):
    # Import model class
    model_class = get_object("sklearn." + info["library"])

    # Instantiate model class
    model = model_class(**model_parameters)
    model_parameters = model.get_params()

    # Set RNG
    info["seed"] = np.random.randint(2 ** 31) if info["seed"] is None else info["seed"]
    np.random.seed(info["seed"])
    return model, info, model_parameters


def generate_pipeline(pipeline_info):
    pipeline = []

    for i, transformation in enumerate(pipeline_info):
        args = transformation.get("args", [])
        kwargs = transformation.get("kwargs", {})
        if "transform" in transformation.keys():
            # Load transformation
            transformer = get_object("sklearn." + transformation["transform"])

            # Instantiate transfomation
            transform = transformer(*args, **kwargs)
        elif "function" in transformation.keys():
            # Load function
            function = get_object(transformation["function"])

            # Wrap fuction to add positional args
            if len(args) > 0:
                function = functools.partial(function, *args, **kwargs)
                kwargs = {}

            # Instantiate transfomation
            transform = FunctionTransformer(function, kw_args = kwargs)
        else:
            raise RuntimeError(f"Ill-formed transformation in position {i}")

        # Add column filter
        columns = transformation.get("columns", None)
        if columns is not None:
            transform = ColumnTransformer([
                ("transform", transform, columns)
            ], remainder="passthrough")

        # Add to the pipeline
        pipeline.append((str(i), transform))
    return pipeline


def pipe_model(pipeline, model):
    if len(pipeline) > 0:
        model = Pipeline(pipeline + [("model", model)])
    return model


def train_model(model, model_target, train):
    model.fit(train.drop(model_target, axis="columns"), train[model_target])
    return model


def test_model(model, model_type, model_target, test):
    truth = test[model_target].to_numpy()
    predicted = model.predict(test.drop(model_target, axis="columns"))
    predicted = predicted.reshape(truth.shape)
    probability = None
    if model_type in ["categorical", "binary"]:
        probability = model.predict_proba(test.drop(model_target, axis="columns"))
        probability = probability.reshape((*truth.shape, -1))
    if model_type in ["binary"]:
        probability = probability[:, 1]
    return truth, predicted, probability


def evaluate_results(model_type, truth, predicted, probability):
    final_metrics = {}

    # Categorical / Binary measures
    if model_type in ["categorical", "binary"]:
        final_metrics["confussion_matrix"] = metrics.confusion_matrix(truth, predicted)
        final_metrics["accuracy"] = metrics.accuracy_score(truth, predicted)
        final_metrics["balanced_accuracy"] = metrics.balanced_accuracy_score(truth, predicted)
        final_metrics["precision"] = metrics.precision_score(truth, predicted, average=None)
        final_metrics["recall"] = metrics.recall_score(truth, predicted, average=None)
        final_metrics["AUC"] = metrics.roc_auc_score(truth, probability, average=None)

    # Regresion measures
    if model_type in ["regression"]:
        pass # @TODO

    # General metrics
    final_metrics["truth"] = truth
    final_metrics["predicted"] = predicted

    return final_metrics


def save_model(model, parameters, final_metrics, info, *, overwrite=False):
    # Path and file names for saving models
    instance_path = os.path.join(
        model_path,
        info['analysis'],
        info['data'],
        info['instance'])
    file_parameters = "hyperparameters"
    file_model = "model"
    file_metrics = "performance"

    # Create instance folder
    if os.path.exists(instance_path):
        shutil.rmtree(instance_path)
    os.mkdir(instance_path)

    # Save complex variables
    info["parameters_file"] = os.path.join(instance_path, file_parameters + ".json")
    with open(info["parameters_file"], mode='w', encoding='utf-8') as handler:
        json.dump(parameters, handler)

    info["model_file"] = os.path.join(instance_path, file_model + extension)
    with open(info["model_file"], mode='wb') as handler:
        dump(model, handler)

    info["metrics_file"] = os.path.join(instance_path, file_metrics + extension)
    with open(info["metrics_file"], mode='wb') as handler:
        dump(final_metrics, handler)

    # Save simple values and file paths
    with duckdb.connect(database=model_path + db_file, read_only=False) as con:
        con.execute("""
    CREATE TABLE IF NOT EXISTS Models (
        analysis VARCHAR NOT NULL,
        data VARCHAR NOT NULL,
        instance VARCHAR NOT NULL,
        model_file VARCHAR NOT NULL,
        library VARCHAR NOT NULL,
        parameters_file VARCHAR NOT NULL,
        seed INTEGER NOT NULL,
        metrics_file VARCHAR NOT NULL,
        PRIMARY KEY (analysis, data, instance)
    )
    """)
        # Save instance details (instance name, data used, etc.)
        info_df = pd.DataFrame({key: [value] for key, value in info.items()})
        try:
            con.execute("INSERT INTO Models BY NAME SELECT * FROM info_df")
        except duckdb.ConstraintException:
            if overwrite:
                # Overwrite
                updated = ", ".join([
                    f"{name} = '{info[name]}'" for name in
                    ['model_file', 'library', 'parameters_file', 'metrics_file']
                ])
                updated += f", seed = {info['seed']}"
                key_matches = f"analysis = '{info['analysis']}' AND " \
                    f"data = '{info['data']}' AND " \
                    f"instance = '{info['instance']}'"
                con.execute(f"UPDATE Models SET {updated} WHERE {key_matches}")
            else:
                raise


def load_model(analysis, dataset, instance):
    # Load simple values and file paths
    try:
        with duckdb.connect(database=model_path + db_file, read_only=True) as con:
            #info_df = pd.DataFrame({key: [value] for key, value in info.items()})
            key_matches = f"analysis = '{analysis}' AND " \
                f"data = '{dataset}' AND " \
                f"instance = '{instance}'"
            info_df = con.execute(f"SELECT Models WHERE {key_matches}").df()
            info = info_df.iloc[0].to_dict()
    except (IndexError, duckdb.CatalogException) as err:
        raise RuntimeError("Model not present in database") from err

    # Load complex variables
    with open(info["parameters_file"], mode='r', encoding='utf-8') as handler:
        parameters = json.load(handler)

    with open(info["model_file"], mode='rb') as handler:
        model = load(handler)

    with open(info["metrics_file"], mode='rb') as handler:
        final_metrics = load(handler)

    return model, parameters, final_metrics, info


class Model():
    def __init__(self, info, params):
        # Save variables to local
        self.info = info.copy()
        self.params = params.copy()
        self.model = None
        self.metrics = None
        self.train, self.test = None, None

        # Load analysis JSON info
        with open(sandbox_path + "analysis.json", mode='r', encoding="utf-8") as handler:
            analysis = json.load(handler)
            self.analysis = analysis[self.info["analysis"]]

        # Load dataset JSON info
        with open(features_path + "dataset.json", mode='r', encoding="utf-8") as handler:
            dataset = json.load(handler)
            self.dataset = dataset[self.info["analysis"]][self.info["data"]]

    def build(self):
        self.model, self.info, self.params = generate_model(self.info, self.params)
        pipeline = generate_pipeline(self.dataset.get("data_preparation", []))
        self.model = pipe_model(pipeline, self.model)
        return self

    def fit(self):
        self._load_data()
        train_model(self.model, self.dataset["target"], self.train)
        return self

    def validate(self):
        self._load_data()
        model_type = self.analysis["type"]
        model_target = self.dataset["target"]
        truth, pred, prob = test_model(self.model, model_type, model_target, self.test)
        self.metrics = evaluate_results(model_type, truth, pred, prob)
        return self

    def predict(self, data):
        return self.model.predict(data)

    def save(self):
        save_model(self.model, self.params, self.metrics, self.info)

    @staticmethod
    def load(analysis, dataset, model):
        model, params, final_metrics, info = load_model(analysis, dataset, model)
        new = Model(info, params)
        new.model = model
        new.metrics = final_metrics
        return new

    def _load_data(self):
        if self.train is None or self.test is None:
            self.train, self.test = load_data(self.info)
    