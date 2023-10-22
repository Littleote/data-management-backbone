# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import os
import duckdb
import pandas as pd

persistent_path = 'landing/persistent/'
formatted_path = 'formatted/'

def check_tables(db_file):
    if os.path.isfile(formatted_path + db_file):
        with duckdb.connect(database=formatted_path + db_file, read_only=True) as con:
            tables = con.execute("SHOW ALL TABLES").df()
            tables = list(tables['name'])
    else:
        duckdb.connect(database=formatted_path + db_file).close()
        tables = []
    return tables


def check_files(dir_path):
    dir_files = os.listdir(persistent_path + dir_path)
    names = list(map(lambda f: f.split('.')[0], dir_files))
    return (dir_files, names)


def load_dataset(missing, dir_path, args):
    for dir_file, db_name in missing:
        yield db_name, pd.read_csv(persistent_path + dir_path + dir_file, **args)


def to_formatted(pipeline, target):
    dir_path = target + '/'
    files, names = check_files(dir_path)
    db_file = target + '.db'
    tables = check_tables(db_file)
    missing = [(f, n) for f, n in zip(files, names) if n not in tables]

    if len(missing) != 0:
        with duckdb.connect(database=formatted_path + db_file, read_only=False) as con:
            read_args = pipeline.get("read", {})
            for name, df in load_dataset(missing, dir_path, read_args):
                con.execute(f'CREATE TABLE {name} AS SELECT * FROM df')
