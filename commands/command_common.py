# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import json
import duckdb
import models as model_func
from utils import select, confirm, new_name

DB_FILE = 'database.db'

def query_analysis_parameters(info):
    no_opt = "<CANCEL>"
    analysis_type = None
    while analysis_type is None:
        analysis_type = select("analysis type", [no_opt, "regression", "categorical", "binary"])
    if analysis_type == no_opt:
        return
    info["type"] = analysis_type

    # Get possible columns to use
    with duckdb.connect(f"exploitation/{DB_FILE}", read_only=True) as con:
        tables = con.execute("SHOW ALL TABLES").df()
    for _, row in tables.iterrows():
        if confirm(f"Use table {row['name']} for the analysis?"):
            no_opt = "<CONTINUE>"
            all_opt = "<ALL>"
            special_opt = [no_opt, all_opt]

            # Column select
            col_names = row["column_names"]
            selection = []
            variable = ""
            options = special_opt + col_names
            while variable not in special_opt:
                variable = select("column", options)
                if variable is not None:
                    options.remove(variable)
                    selection.append(variable)
            if variable == all_opt:
                selection = '*'
            elif variable == no_opt:
                selection.remove(no_opt)
            info['data'][row['name']] = selection

            # Constraint addition
            if confirm(f"Use only a subset of table {row['name']} for the analysis?",
                       default=False):
                sql_constraint = input(
                    "Now write the subset selection code (SQL WHERE clause): ")
                info['constraints'][row['name']] = sql_constraint
    return info


def query_dataset_parameters(info, analysis_name):
    # Get possible columns to use
    with duckdb.connect(f"sandbox/{DB_FILE}", read_only=True) as con:
        tables = con.execute("SHOW ALL TABLES").df()
        tables = tables[tables["schema"] == analysis_name]

    # Give the tables and columns of the analysis
    print(f"Analysis {analysis_name} table information:")
    for _, row in tables.iterrows():
        print(f"Table {row['name']}")
        print(*[f"{i + 1}. {val}" for i, val in enumerate(row['column_names'])], sep = "\n")

    # Get transfomation query
    print("Enter SQL command.")
    print("Press enter twice to finish.")
    query_line = None
    query = ""
    while query_line != "":
        query_line = input()
        query += '\n' + query_line
    query = query[1:-1]
    info["transform"] = query

    # Ask user for the target(s) column(s)
    target = input("Indicate the target column " \
                               "(in case of multiple, separate them with a coma)\n")
    targets = target.replace(' ', '').split(',')
    info["target"] = target if len(targets) == 1 else targets

    # Ask the user for the splitting ratio
    try:
        info["split"] = float(input("Indicate the ratio to use for training\n"))
        assert 0 < info["split"] < 1
    except(ValueError, AssertionError) as err:
        raise ValueError("Invalid vale for a ratio") from err
    
    return info


def query_dataset_transforms(info):
    raise NotImplementedError()


def query_model_definition(info):
    try:
        with duckdb.connect(f"models/{DB_FILE}", read_only=True) as con:
            key_matches = f"analysis = '{info['analysis']}' AND " \
                f"data = '{info['data']}'"
            matches = con.execute(f"SELECT Models WHERE {key_matches}").df()
            matches = list(matches["instance"])
    except duckdb.CatalogException:
        matches = []

    # Introduce the new name
    name = new_name("model instance", matches)
    if name is None:
        return
    info["instance"] = name
    
    print("Specify module path for the model class")
    library = input("sklearn.")
    try:
        model_func.get_object("sklearn." + library)
        valid_import = True
    except (AttributeError):
        valid_import = False
        print(f"{library} is not a valid object")
    while not valid_import or not confirm(f"The model class is sklearn.{library}?"):
        print("Specify module path for the model class")
        library = input("sklearn.")
        try:
            model_func.get_object("sklearn." + library)
            valid_import = True
        except (AttributeError):
            valid_import = False
    info["library"] = name
    
    if confirm("Set a seed for the model?", default=False):
        try:
            seed = input("Seed: ")
            info["seed"] = int(seed)
        except ValueError:
            info["seed"] = hash(seed) // (2 ** 31)
    else:
        info["seed"] = info.get("seed", None)
    
    raise NotImplementedError()
    return info, {}