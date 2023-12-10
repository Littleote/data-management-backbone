# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import json
import duckdb
import numpy as np

sandbox_path = 'sandbox/'
features_path = 'feature_generation/'
db_file = 'database.db'

def transform_analysis_data(analysis_name, data_name):
    with open(features_path + "dataset.json", mode='r', encoding="utf-8") as handler:
        data_set = json.load(handler)

    target_data = data_set[analysis_name][data_name]
    sql_transform = target_data["transform"]

    with duckdb.connect(database=sandbox_path + db_file, read_only=False) as con:
        con.execute(f"ATTACH DATABASE '{features_path + db_file}' AS features")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS features.{analysis_name}")
        con.execute(f"USE database.{analysis_name}")
        con.execute(f"CREATE OR REPLACE TABLE features.{analysis_name}.{data_name} " \
                    f"AS {sql_transform}")


def split_analysis_data(analysis_name, data_name):
    with open(features_path + "dataset.json", mode='r', encoding="utf-8") as handler:
        data_set = json.load(handler)

    target_data = data_set[analysis_name][data_name]
    split = target_data["split"]

    with duckdb.connect(database=features_path + db_file, read_only=False) as con:
        full_data = con.execute(f"SELECT * FROM {analysis_name}.{data_name}").df()
        size = len(full_data)
        split_point = int(size * split)
        index = np.random.permutation(size)
        train_data = full_data.iloc[index[:split_point]]
        test_data = full_data.iloc[index[split_point:]]
        con.execute(f"CREATE OR REPLACE TABLE {analysis_name}.{data_name}_train " \
                    f"AS SELECT * FROM train_data")
        con.execute(f"CREATE OR REPLACE TABLE {analysis_name}.{data_name}_test " \
                    f"AS SELECT * FROM test_data")
