# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import os
import json
import duckdb

import exploitation
import sandbox
import feature_generation as fgeneration

from utils import Path, confirm, select, get_zones

DB_FILE = 'database.db'


def update(kind, folder):
    if kind == "table":
        update_table(folder)
    elif kind == "analysis":
        update_analysis(folder)
    elif kind == "dataset":
        update_dataset(folder)
    else:
        raise NotImplementedError(f"update {kind} not yet implemented")


def check_folders(folder):
    with Path(folder):
        for zone in get_zones():
            os.makedirs(zone, exist_ok=True)


def update_table(folder):
    # Define json path
    file_path = os.path.join(folder, "exploitation", "tables.json")

    # Open file and write the data with the new table
    with open(file_path, mode='r', encoding='utf-8') as handler:
        data = json.load(handler)

    # Introduce the new table and SQL code
    name_table = select("table", list(data.keys()))
    if name_table is None:
        print("Update canceled")
        return
    sql_code = input("Now insert the new SQL code needed to create the table: ")

    # Confirm updated query
    print("Change from OLD:")
    print(data[name_table])
    print("to NEW:")
    print(sql_code)
    if not confirm(None):
        print("Update canceled")
        return
    data[name_table] = sql_code

    # Open file and write the data with the new table
    with open(file_path, mode='w', encoding='utf-8') as file:
        json.dump(data, file, indent=4)

    # Rerun exploitation
    exploitation.merge_tables({name_table: sql_code})

    # Confirmation
    print("Your table has been succesfully updated")


def update_analysis(folder):
    with Path(folder):
        # Get list of analysis in use (with models)
        try:
            with duckdb.connect(f"models/{DB_FILE}", read_only=False) as con:
                used = con.execute("SELECT DISTINCT analysis FROM Models").df()
                used = list(used["analysis"])
        except duckdb.CatalogException:
            used = []

        # Open file and read data
        if os.path.isfile("sandbox/analysis.json"):
            with open("sandbox/analysis.json", mode='r', encoding='utf-8') as handler:
                analysis = json.load(handler)
        else:
            print("No analysis present to update")
            return

        # Introduce the new name
        name = select("analysis", list(analysis.keys()))
        if name is None:
            print("Update analysis aborted")
            return

        info = analysis[name]

        if name in used:
            update_params = False
            if confirm(f"{name} is already in use by a model, create a copy insetad?",
                       force_confirmation=True):
                # Introduce the new name
                name = input("Insert the name of the copy: ")
                if name in analysis.keys():
                    print("This name is already in use, insert a name not in:")
                    print(*list(analysis.keys()), sep=',')
                    name = input("Name of the new copy: ")
                    if name in analysis.keys():
                        print("Name already present")
                        return
            else:
                print("Update analysis aborted")
                return
        else:
            update_params = confirm("Update analysis parameters?", force_confirmation=True)

        if update_params:
            analysis_type = select("analysis type", ["regression", "categorical", "binary"])
            if analysis_type is None:
                print("Update analysis aborted")
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
            analysis[name] = info

            # Open file and write the data with the new table
            with open("sandbox/analysis.json", mode='w', encoding='utf-8') as handler:
                json.dump(analysis, handler, indent=4)

            # Confirmation
            print("The parameters have been succesfully updated")

        # Fetch analysis tables
        sandbox.fetch_analysis_data(name)

        # Confirmation
        print("The analysis tables have been updated")


def update_dataset(folder):
    with Path(folder):
        # Get list of analysis in use (with models)
        try:
            with duckdb.connect(f"models/{DB_FILE}", read_only=False) as con:
                used = con.execute("SELECT DISTINCT analysis, dataset FROM Models").df()
                used = list(zip(used["analysis"], used["dataset"]))
        except duckdb.CatalogException:
            used = []

        # Open analysis file, read data and select analysis
        if os.path.isfile("feature_generation/dataset.json"):
            with open("feature_generation/dataset.json", mode='r', encoding='utf-8') as handler:
                dataset = json.load(handler)
        else:
            print("Create an dataset first")
            return

        # Select dataset (and analysis) to update
        options = {
            f"{data} ({analy})": (analy, data)
            for analy, entry in dataset.items()
            for data in entry.keys()
         }

        # Introduce the new name
        option = select("dataset", list(options.keys()))
        if option is None:
            print("Update dataset aborted")
            return
        option = options[option]
        analysis_name = option[0]
        name = option[1]
        data_subset = dataset[analysis_name]

        info = data_subset[name]

        if name in used:
            update_params = False
            if confirm(f"{name} is already in use by a model, create a copy insetad?",
                       force_confirmation=True):
                # Introduce the new name
                name = input("Insert the name of the copy: ")
                if name in data_subset.keys():
                    print("This name is already in use, insert a name not in:")
                    print(*list(data_subset.keys()), sep=',')
                    name = input("Name of the new copy: ")
                    if name in data_subset.keys():
                        print("Name already present")
                        return
            else:
                print("Update dataset aborted")
                return
        else:
            update_params = confirm("Update analysis parameters?", force_confirmation=True)

        if update_params:
            # Open analysis file and read data
            with open("sandbox/analysis.json", mode='r', encoding='utf-8') as handler:
                analysis = json.load(handler)

            # Give the tables and columns of the analysis
            print(f"Analysis {analysis_name} informatio:")
            for table, rows in analysis[analysis_name]['data'].items():
                print(f"Table {table}")
                print(*[f"{i + 1}. {val}" for i, val in enumerate(rows)], sep = "\n")

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
            except(ValueError, AssertionError):
                print("Invalid vale for a ratio")
                return

            # Add results
            data_subset[name] = info
            dataset[analysis_name] = data_subset

            # Open file and write the data with the new table
            with open("feature_generation/dataset.json", mode='w', encoding='utf-8') as handler:
                json.dump(dataset, handler, indent=4)

            # Confirmation
            print("The parameters have been succesfully updated")

        # Fetch analysis tables
        fgeneration.transform_analysis_data(analysis_name, name)
        fgeneration.split_analysis_data(analysis_name, name)

        # Confirmation
        print("The dataset tables have been created")
