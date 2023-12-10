# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import os
import shutil
from pickle import dump
import importlib
import duckdb
import numpy as np
import pandas as pd

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


def generate_model(info, model_parameters):
    # Import model clas
    module_name, class_name = info["library"].split('.')
    module = importlib.import_module("sklearn." + module_name)
    model_class = getattr(module, class_name)

    # Instantiate model class
    model = model_class(**model_parameters)
    model_parameters = model.get_params()

    # Set RNG
    info["seed"] = np.random.randint(2 ** 31) if info["seed"] is None else info["seed"]
    np.random.seed(info["seed"])
    return model, info, model_parameters


def train_model(model, model_target, train):
    model.fit(train.drop(model_target, axis="columns"), train[model_target])
    return model


def test_model(model, model_target, test):
    truth = test[model_target].to_numpy()
    predicted = model.predict(test.drop(model_target, axis="columns")).reshape(truth.shape)
    return truth, predicted


def evaluate_results():
    pass # @TODO


def save_model(model, parameters, metrics, info):
    # Path and file names for saving models
    instance_path = model_path + info["instance"] + '/'
    file_parameters = "hyperparameters"
    file_model = "model"
    file_metrics = "performance"

    # Create instance folder
    if os.path.exists(instance_path):
        shutil.rmtree(instance_path)
    os.mkdir(instance_path)

    # Save complex variables
    info["parameters_file"] = instance_path + file_parameters + extension
    with open(info["parameters_file"], mode='wb') as handler:
        dump(parameters, handler)

    info["model_file"] = instance_path + file_model + extension
    with open(info["model_file"], mode='wb') as handler:
        dump(model, handler)

    info["metrics_file"] = instance_path + file_metrics + extension
    with open(info["metrics_file"], mode='wb') as handler:
        dump(metrics, handler)

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
        info_df = pd.DataFrame({key: [value] for key, value in info.items()})
        con.execute("INSERT INTO Models BY NAME SELECT * FROM info_df")
