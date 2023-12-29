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

from utils import Path, confirm, select, new_name, get_zones

from .command_common import query_analysis_parameters
from .command_common import query_dataset_parameters, query_dataset_transforms
# from .command_common import query_model_definition, query_model_parametrization

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
            if confirm(f"{name} is already in use by a model, create a copy insetad?",
                       force_confirmation=True):
                # Introduce the new name
                name = new_name("analysis", list(analysis.keys()))
                if name is None:
                    print("Update analysis aborted")
                    return
            else:
                print("Update analysis aborted")
                return

        if confirm("Update analysis parameters?", force_confirmation=True):
            # Ask the user for the parameters
            try:
                info = query_analysis_parameters(info)
                if info is None:
                    print("Update analysis aborted")
                    return
            except() as err:
                print(*err.args, sep="\n")
                return

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
            if confirm(f"{name} is already in use by a model, create a copy insetad?",
                       force_confirmation=True):
                # Introduce the new name
                name = new_name("dataset", list(data_subset.keys()))
                if name is None:
                    print("Update dataset aborted")
                    return
            else:
                print("Update dataset aborted")
                return

        if confirm("Update analysis parameters?", force_confirmation=True):
            # Ask the user for the parameters
            try:
                info = query_dataset_parameters(info, analysis_name)
            except(ValueError) as err:
                print(*err.args, sep="\n")
                return

            # Add results
            data_subset[name] = info
            dataset[analysis_name] = data_subset

            # Open file and write the data with the new table
            with open("feature_generation/dataset.json", mode='w', encoding='utf-8') as handler:
                json.dump(dataset, handler, indent=4)

            # Confirmation
            print("The parameters have been succesfully updated")

        if confirm("Update extra transformations?", default=False):
            # Ask the user for the transforms
            try:
                info = query_dataset_transforms(info)
            except() as err:
                print(*err.args, sep="\n")
                return

            # Add results
            data_subset[name] = info
            dataset[analysis_name] = data_subset

            # Open file and write the data with the new table
            with open("feature_generation/dataset.json", mode='w', encoding='utf-8') as handler:
                json.dump(dataset, handler, indent=4)

            # Confirmation
            print("The transformations have been succesfully updated")

        # Fetch analysis tables
        fgeneration.transform_analysis_data(analysis_name, name)
        fgeneration.split_analysis_data(analysis_name, name)

        # Confirmation
        print("The dataset tables have been created")
