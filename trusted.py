# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import os
import duckdb

formatted_path = 'formatted/'
trusted_path = 'trusted/'
db_trusted = trusted_path + 'database.db'

def check_database():
    if not os.path.isfile(db_trusted):
        duckdb.connect(database=db_trusted, read_only=False).close()


def get_schema(target, rename, keys):
    db_target = formatted_path + target + '.db'
    with duckdb.connect(database=db_target, read_only=False) as con:
        table_info = con.execute("SHOW ALL TABLES").df()
        table_info.set_index('name', inplace=True)
        table_names = list(table_info.index)

        # Get table variables
        variables = set(sum(table_info['column_names'], []))
        discarded = set(rename.keys())
        variables = (variables - discarded).union(
            [rename[name] for name in variables.intersection(discarded)])
        assert all(map(lambda key: key in variables, keys)), \
            f"Not all keys appear in the dataset \n keys: {keys} \n variables: {variables}"

        # Get variable types
        var_info = {}
        for table in table_names:
            var_names = table_info.loc[table, 'column_names']
            var_types = table_info.loc[table, 'column_types']
            for name, typing in zip(var_names, var_types):
                name = rename.get(name, name)
                var_info[name] = typing

            # Prepare new table
            table_columns = ', '.join([var_name + ' ' + var_type
                                       for var_name, var_type in var_info.items()])
            table_keys = ', '.join(list(keys))
    return table_columns, table_keys


def join_versions(target, table_columns, table_keys, rename, keep_latest):
    db_target = formatted_path + target + '.db'
    with duckdb.connect(database=db_trusted, read_only=False) as con:
        # Attach tables and extract information
        con.execute(f"ATTACH DATABASE '{db_target}' AS source")
        table_info = con.execute("SHOW ALL TABLES").df()
        table_info = table_info.loc[table_info['database'] == 'source']
        table_info.set_index('name', inplace=True)
        table_names = list(table_info.index)
        table_names.sort(reverse=keep_latest)

        # Create new table
        con.execute(f"CREATE OR REPLACE TABLE {target}({table_columns}, PRIMARY KEY({table_keys}))")

        # Join new tables
        for name in table_names:
            variables = list(table_info.loc[name, 'column_names'])
            new_variables = [rename.get(name, name) for name in variables]
            renaming = ", ".join(f"#{i + 1} AS {new}" for i, new in enumerate(new_variables))
            con.execute(f"INSERT OR IGNORE INTO {target} SELECT {renaming} FROM source.{name}")


def to_trusted(pipeline, target):
    rename = pipeline.get("rename", {})
    keys = pipeline.get("keys", [])
    keep = pipeline.get("keep", "latest")
    keep_latest = keep == "latest"
    check_database()
    table_columns, table_keys = get_schema(target, rename, keys)
    join_versions(target, table_columns, table_keys, rename, keep_latest)
