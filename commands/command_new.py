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

from utils import Path, select, get_zones


def new(kind, folder):
    if kind == "pipeline":
        new_pipeline(folder)
    elif kind == "table":
        new_table(folder)
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
            print(f"Is '{filename}' the expected file?")
            option = input("[Y]/N\n")
            if len(option) > 0 and option.lower()[0] == 'n':
                retry = True
        except RuntimeError:
            print("To get the download URL you can hover over the download button, " \
                  "right click and select copy link.")
            retry = True
    pipeline['URL'] = url
# =============================================================================
#     print("Is it necessary to unfold the file?")
#     option = input("Y/[N]\n")
#     if len(option) > 0 and option.lower()[0] == 'y':
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
    print("Keep latest version?")
    option = input("[Y]/N\n")
    if len(option) > 0 and option.lower()[0] == 'n':
        pipeline["keep"] = "oldest"

    # Run step
    try:
        trusted.to_trusted(pipeline, name)
    except Exception as err:
        raise RuntimeError() from err
    return pipeline


def new_pipeline_clening(pipeline, name):
    transformation = []
    with duckdb.connect("trusted/database.db", read_only=False) as con:
        table = con.execute(f"SELECT * FROM {name}").df()
        print("Current table:")
        print(table.head().to_string())
        print(table.head().to_string())
        print("Add SQL to cleaning step?")
        option = input("Y/[N]\n")
        retry = len(option) > 0 and option.lower()[0] == 'y'
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
            print("Add another SQL to cleaning step?")
            option = input("Y/[N]\n")
            retry = len(option) > 0 and option.lower()[0] == 'y'
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
