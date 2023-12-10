# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import json
import duckdb

exploitation_path = 'exploitation/'
sandbox_path = 'sandbox/'
db_file = 'database.db'

def fetch_analysis_data(target):
    with open(sandbox_path + "analysis.json", mode='r', encoding="utf-8") as handler:
        analysis_set = json.load(handler)

    target_analysis = analysis_set[target]
    analysis_name = target
    analysis_data = target_analysis["data"]
    analysis_data_constraints = target_analysis["constraints"]

    with duckdb.connect(database=sandbox_path + db_file, read_only=False) as con:
        con.execute(f"ATTACH DATABASE '{exploitation_path + db_file}' AS source")
        for table, features in analysis_data.items():
            if isinstance(features, list):
                selection = ", ".join(features)
            else:
                selection = features
            constraints = analysis_data_constraints.get(table, None)
            if constraints is None:
                constraints = ""
            else:
                constraints = f"WHERE {constraints}"
            con.execute(f"CREATE SCHEMA IF NOT EXISTS database.{analysis_name}")
            con.execute(f"USE database.{analysis_name}")
            con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT {selection} " \
                        f"FROM source.{table} {constraints}")
