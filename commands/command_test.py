# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import duckdb
import pandas as pd

from models import Model

from utils import EVAL_RESTRICTIONS, Path, select

def test(kind, folder):
    if kind == "input":
        test_input(folder)
    else:
        raise NotImplementedError(f"update {kind} not yet implemented")

def test_input(folder):
    with Path(folder):
        # Open analysis file, read data and select analysis
        try:
            with duckdb.connect("model/database.db", read_only=True) as con:
                matches = con.execute("SELECT * FROM Models").df()
                instances = [
                    (row["analysis"], row["data"], row["instance"], index)
                    for index, row in matches.iterrows()
                ]
        except duckdb.CatalogException:
            matches = []

        # Select model instance (and analysis [and dataset])
        options = {
            f"{instance[2]} ({instance[0]}[{instance[1]}])": instance
            for instance in instances
         }

        option = select("model", list(options.keys()))
        if option is None:
            print("Model copy aborted")
            return
        option = options[option]
        
        try:
            entries = int(input("Number of input entries: "))
            assert(entries > 0)
        except (ValueError, AssertionError):
            print("Value must be a positie integer")
            return
        
        model = Model.load(*option[:3])
        model.load_data()
        input_data = {}
        dataset = model.train[model.model.feature_names_in_]
        for series_name, series in dataset.items():
            head = map(str, series.unique()[:3])
            print(f"{series_name} ({series.dtype}, example: {', '.join(head)})")
            if entries == 1:
                print("Input the entry: ")
            else:
                print("Input the {entries} entries: ")
            input_data[series_name] = [input() for _ in range(entries)]
        input_data = pd.DataFrame(input_data)
        input_data = input_data.astype(dataset.dtypes)
        prediction = model.predict(input_data)
        print(prediction)
