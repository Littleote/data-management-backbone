# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import os
import json
import tempfile
import pandas as pd
import duckdb

import landing
import formatted
import trusted
import sandbox
import feature_generation as fgeneration
from models import Model

from utils import Path, confirm, select, new_name, get_zones

DB_FILE = 'database.db'


def new(kind, folder):
    if kind == "pipeline":
        new_pipeline(folder)
    elif kind == "table":
        new_table(folder)
    elif kind == "analysis":
        new_analysis(folder)
    elif kind == "dataset":
        new_dataset(folder)
    elif kind == "model":
        new_model(folder)
    else:
        raise NotImplementedError(f"new {kind} not yet implemented")


def check_folders(folder):
    with Path(folder):
        for zone in get_zones():
            os.makedirs(zone, exist_ok=True)


def new_pipeline(folder):
    with tempfile.TemporaryDirectory() as tmp_folder, Path(tmp_folder):
        # Useful bits
        check_folders(tmp_folder)
        print("Press Ctrl + C at any moment to cancel.")

        # Introduce the new pipeline name
        pipeline_name = input("Insert the name of the new table:\n")
        pipeline = {}

        try:
            pipeline, filename = new_pipeline_landing(
                pipeline, pipeline_name)
            pipeline, col_names = new_pipeline_formatted(
                pipeline, pipeline_name, filename)
            pipeline = new_pipeline_trusted(pipeline, pipeline_name, col_names)
            pipeline = new_pipeline_clening(pipeline, pipeline_name)
        except RuntimeError:
            return

        # Save
        with open(os.path.join(folder, 'dataset_info', pipeline_name + '.json'),
                  mode='w', encoding='utf-8') as handler:
            json.dump(pipeline, handler, indent=4)

        # Confirmation
        print("Your new pipeline has been succesfully added")


def new_pipeline_landing(pipeline, name):
    retry = True
    while retry:
        retry = False
        url = input("Download URL:\n")
        try:
            response = landing.request(url)
            landing.check_folders()
            filename = landing.save_request(response)
            if not confirm(f"Is '{filename}' the expected file?"):
                retry = True
        except RuntimeError:
            print("To get the download URL you can hover over the download button, " \
                  "right click and select copy link.")
            retry = True
    pipeline['URL'] = url
# =============================================================================
#     if confirm("Is it necessary to unfold the file?", default=False):
#         [os.path.join(r, e)[len("landing/temporal/"):] for e in f \
#             for r, _, f in os.walk("landing/temporal/")]
#         raise NotImplementedError("unfold parameter specification is not implemented")
# =============================================================================

    # Run step
    try:
        filename = landing.to_landing(pipeline, name)
    except Exception as err:
        raise RuntimeError() from err
    return pipeline, filename

def new_pipeline_formatted(pipeline, name, filename):
    retry = True
    read_args = {}
    print("'param_name: param_value' to add parameter to read_csv, " \
          "'-param_value' to remove, " \
          "'help' for description, " \
          "'current' to see all current parameters "
          "'view' to previsualize the result " \
          "and 'continue' to end")
    while retry:
        value = input("> ")
        if len(value) <= 0:
            continue
        if value == "help":
            help(pd.read_csv)
        elif value == "view":
            try:
                _, df_view = list(formatted.load_dataset(
                    [(filename, None)], name + '/', read_args))[0]
                print(df_view.head().to_string())
                print(df_view.tail().to_string())
            except Exception as err:
                for arg in err.args:
                    print(arg)
        elif value == "current":
            print(read_args)
        elif value == "continue":
            retry = False
        elif value[0] == '-':
            read_args[value[1:]] = None
            read_args.pop(value[1:])
        elif ':' in value:
            value = value.split(':')
            try:
                read_args[value[0]] = eval(':'.join(value[1:]),
                                           None, None)
            except Exception as err:
                print("Invalid parameter value")
                for arg in err.args:
                    print(arg)
        else:
            print("Unknown option")
    pipeline['read'] = read_args

    # Run step
    try:
        formatted.to_formatted(pipeline, name)
        _, df_view = list(formatted.load_dataset(
            [(filename, None)], name + '/', read_args))[0]
    except Exception as err:
        raise RuntimeError() from err
    return pipeline, df_view.columns.to_list()


def new_pipeline_trusted(pipeline, name, col_names):
    no_opt = "<CONTINUE>"

    # Variable renaming
    rename = {}
    variable = ""
    options = [no_opt] + col_names
    while variable != no_opt:
        variable = select("renaming option", options)
        if variable is not None and variable != no_opt:
            rename[variable] = input("New name (blank to undo): ")
            if rename[variable] == "":
                rename[variable].pop()
    new_names = [rename.get(name, name) for name in col_names]
    pipeline["rename"] = rename

    # Key values for join
    keys = []
    key = ""
    options = [no_opt] + new_names
    while key != no_opt:
        key = select("key variable", options)
        if key is not None and key != no_opt:
            keys += [key]
            options.remove(key)
    pipeline["keys"] = keys

    # Order
    pipeline["keep"] = "latest"
    if not confirm("Keep latest version?"):
        pipeline["keep"] = "oldest"

    # Run step
    try:
        trusted.to_trusted(pipeline, name)
    except Exception as err:
        raise RuntimeError() from err
    return pipeline


def new_pipeline_clening(pipeline, name):
    transformation = []
    with duckdb.connect(f"trusted/{DB_FILE}", read_only=False) as con:
        table = con.execute(f"SELECT * FROM {name}").df()
        print("Current table:")
        print(table.head().to_string())
        print(table.head().to_string())
        retry = confirm("Add SQL to cleaning step?", default=False)
        while retry:
            # Get query
            print("Enter SQL command (you may use {dataset} as the table name).")
            print("Press enter twice to finish.")
            query_line = None
            query = ""
            while query_line != "":
                query_line = input()
                query += '\n' + query_line
            query = query[1:-1]

            # Test and save query
            try:
                con.execute(query.format(dataset=name))
                table = con.execute(f"SELECT * FROM {name}").df()
                transformation += [query]
                print("Updated table:")
                print(table.head().to_string())
                print(table.head().to_string())

            except (duckdb.CatalogException, duckdb.ParserException) as err:
                print("Unable to run transformation:")
                print(f"{query.format(dataset=name)}")
                print("Causes:")
                for arg in err.args:
                    print(arg)
            retry = confirm("Add another SQL to cleaning step?", default=False)
    pipeline["transformations"] = transformation
    return pipeline


def new_table(folder):
    # Introduce the new table and SQL code
    name_table = input("Insert the name of the new table: ")
    sql_code = input("Now insert the SQL code needed to create the new table: ")

    # Define json path
    file_path = os.path.join(folder, "exploitation", "tables.json")

    # Open file and read data
    with open(file_path, mode='r', encoding='utf-8') as file:
        data = json.load(file)

    # Define the new element
    new_element = {
        name_table: sql_code
    }

    # Add the new element to the data
    data.update(new_element)

    # Open file and write the data with the new table
    with open(file_path, mode='w', encoding='utf-8') as file:
        json.dump(data, file, indent=4)

    # Confirmation
    print("Your new table has been succesfully added")


from .command_common import query_analysis_parameters

def new_analysis(folder):
    with Path(folder):
        # Open file and read data
        if os.path.isfile("sandbox/analysis.json"):
            with open("sandbox/analysis.json", mode='r', encoding='utf-8') as handler:
                analysis = json.load(handler)
        else:
            analysis = {}

        # Introduce the new name
        name = new_name("analysis", list(analysis.keys()))
        if name is None:
            print("New analysis aborted")
            return

        new_info = {
            "data": {},
            "constraints": {},
            "type": None
        }

        # Ask the user for the parameters
        try:
            new_info = query_analysis_parameters(new_info)
            if new_info is None:
                print("New analysis aborted")
                return
        except() as err:
            print(*err.args, sep="\n")
            return

        analysis[name] = new_info

        # Open file and write the data with the new table
        with open("sandbox/analysis.json", mode='w', encoding='utf-8') as handler:
            json.dump(analysis, handler, indent=4)

        # Confirmation
        print("The parameters have been succesfully created")

        # Fetch analysis tables
        sandbox.fetch_analysis_data(name)

        # Confirmation
        print("The analysis tables have been created")


from .command_common import query_dataset_parameters, query_dataset_transforms

def new_dataset(folder):
    with Path(folder):
        # Open analysis file, read data and select analysis
        try:
            with open("sandbox/analysis.json", mode='r', encoding='utf-8') as handler:
                analysis = json.load(handler)
            analysis_name = select("analysis", list(analysis.keys()))
        except FileNotFoundError:
            print("Create an analysis first")
            return

        # Open analysis file, read data and select analysis
        if os.path.isfile("feature_generation/dataset.json"):
            with open("feature_generation/dataset.json", mode='r', encoding='utf-8') as handler:
                dataset = json.load(handler)
        else:
            dataset = {}
        data_subset = dataset.get(analysis_name, {})

        # Introduce the new name
        name = new_name("dataset", list(data_subset.keys()))
        if name is None:
            print("New data_subset aborted")
            return

        new_info = {}

        # Ask the user for the parameters
        try:
            new_info = query_dataset_parameters(new_info, analysis_name)
            if new_info is None:
                print("New data_subset aborted")
                return
        except(ValueError) as err:
            print(*err.args, sep="\n")
            return

        # Add results
        data_subset[name] = new_info
        dataset[analysis_name] = data_subset

        # Open file and write the data with the new table
        with open("feature_generation/dataset.json", mode='w', encoding='utf-8') as handler:
            json.dump(dataset, handler, indent=4)

        # Confirmation
        print("The parameters have been succesfully created")

        if confirm("Add extra transformations?", default=False):
            # Ask the user for the transforms
            try:
                new_info = query_dataset_transforms(new_info, analysis_name)
            except() as err:
                print(*err.args, sep="\n")
                return
    
            # Add results
            data_subset[name] = new_info
            dataset[analysis_name] = data_subset
    
            # Open file and write the data with the new table
            with open("feature_generation/dataset.json", mode='w', encoding='utf-8') as handler:
                json.dump(dataset, handler, indent=4)
    
            # Confirmation
            print("The transformations have been succesfully created")

        # Fetch analysis tables
        fgeneration.transform_analysis_data(analysis_name, name)
        fgeneration.split_analysis_data(analysis_name, name)

        # Confirmation
        print("The dataset tables have been created")


from .command_common import query_model_definition

def new_model(folder):
    with Path(folder):
        # Open analysis file, read data and select analysis
        if os.path.isfile("feature_generation/dataset.json"):
            with open("feature_generation/dataset.json", mode='r', encoding='utf-8') as handler:
                dataset = json.load(handler)
        else:
            print("Create an dataset first")
            return

        # Select dataset (and analysis)
        options = {
            f"{data} ({analy})": (analy, data)
            for analy, entry in dataset.items()
            for data in entry.keys()
         }

        option = select("dataset", list(options.keys()))
        if option is None:
            print("Model creation aborted")
            return
        option = options[option]

        info = {
            "analysis": option[0],
            "data": option[1],
            "instance": None,
            "library": None,
            "seed": None
        }
        
        # Ask the user for the transforms
        try:
            info, params = query_model_definition(info)
        except() as err:
            print(*err.args, sep="\n")
            return
        
        model = Model(info, params)
        model.build().fit().validate().save()
        