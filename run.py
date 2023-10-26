# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import os
import sys
import json
import tempfile
import argparse
import pandas as pd
import duckdb

import landing
import formatted
import trusted
import exploitation

ZONES = ["landing", "formatted", "trusted", "exploitation", "dataset_info"]

def check_folders(folder):
    directories = os.listdir(folder)
    for zone in ZONES:
        if zone not in directories:
            os.mkdir(os.path.join(folder, zone))


def select(name, options):
    if len(options) > 1:
        # Choose if there are multiple
        print(f"Available {name}s:")
        for i, option in enumerate(options):
            print(f"   {i + 1}.- {option}")
        print()
        option = input(f"Select a {name}'s index: ")
        try:
            option = int(option)
            assert 0 < option <= len(options)
        except (AssertionError, ValueError):
            print("Invalid index")
            return None
        return options[option - 1]

    if len(options) == 1:
        # Confirm if there's only one
        print(f"Only {name} available is {options[0]}, procced?")
        option = input("[Y]/N\n")
        if len(option) <= 0 or option.lower()[0] != 'n':
            return options[0]
    # Cancel if not
    return None


def parse(args):
    arg_parser = argparse.ArgumentParser(
        description='Data management pipeline',
        epilog='by David Candela and Andreu Giménez')
    action = arg_parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--fetch", nargs='?', const=-1, metavar="DATASET"
                        , help="Fetch the latest version of a dataset")
    action.add_argument("--new", choices=["pipeline", "table"]
                        , help="Create a new dataset pipeline or a new exploitation table")
    action.add_argument("--view", choices=["formatted", "trusted", "exploitation"]
                        , metavar="ZONE", type = str.lower
                        , help="Visualize a table in the specified zone \n" \
                            "(formatted, trusted or exploitation)")
    return arg_parser.parse_args(args)


def fetch(pipeline, folder):
    # Load Pipeline names
    pipelines = os.listdir(os.path.join(folder, "dataset_info"))
    pipelines = [pipe[:-5] for pipe in pipelines if pipe[-5:] == '.json']
    if pipeline == -1:
        # If not given, select from available
        pipeline = select("pipeline", pipelines)
        if pipeline is None:
            return
    else:
        # If given, check its valid
        if not pipeline in pipelines:
            print(f"{pipeline} pipeline doesn't exist")
            return

    working_directory = os.getcwd()
    os.chdir(folder)
    # Load JSON parameters
    with open(os.path.join(folder, "dataset_info", pipeline + '.json'),
              mode='r', encoding='utf-8') as handler:
        pipeline_info = json.load(handler)

    try:
        # Run pipeline
        print("Fetching dataset               ", end="\r")
        landing.to_landing(pipeline_info, pipeline)
        print("Formatting dataset             ", end="\r")
        formatted.to_formatted(pipeline_info, pipeline)
        print("Joining dataset versions       ", end="\r")
        trusted.to_trusted(pipeline_info, pipeline)
        print("Cleaning dataset               ", end="\r")
        trusted.clean_data(pipeline_info, pipeline)
        print("Generaiting exploitation tables", end="\r")
        exploitation.to_exploitation()
        print("Done                           ", end="\n")
    except RuntimeError:
        os.sys.exit()
    os.chdir(working_directory)


def new_pipeline(folder):
    with tempfile.TemporaryDirectory() as tmp_folder:
        # Useful bits
        check_folders(tmp_folder)
        os.chdir(tmp_folder)
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
            os.chdir(folder)
            return

        # Save
        with open(os.path.join(folder, 'dataset_info', pipeline_name + '.json'),
                  mode='w', encoding='utf-8') as handler:
            json.dump(pipeline, handler, indent=4)
        os.chdir(folder)


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
    option = input("Y/[N]\n")
    if len(option) > 0 and option.lower()[0] == 'y':
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


def view(zone, folder):
    folder = os.path.join(folder, zone)
    if zone == "formatted":
        db_files = os.listdir(folder)
        db_files = [db_file for db_file in db_files if db_file[-3:] == '.db']
        db_connection = select("database", db_files)
        if db_connection is None:
            return
    else:
        db_connection = "database.db"
    print(f"Connecting to {zone}/{db_connection}")
    db_connection = os.path.join(folder, db_connection)
    with duckdb.connect(db_connection, read_only=True) as con:
        tables = con.execute("SHOW ALL TABLES").df()
        tables = tables['name']
        table = select("table", tables)
        if table is None:
            return

        print()
        print(" === SCHEMA INFORMATION === ")
        print(con.execute(f"DESCRIBE {table}").df().to_string())

        print()
        print(" === TABLE STATISTICS === ")
        table_df = con.execute(f"SELECT * FROM {table}").df()
        print(table_df.describe().to_string())

        print()
        print(" === HEAD === ")
        print(table_df.head().to_string())
        print(" === TAIL === ")
        print(table_df.tail().to_string())

        quit_message = "Press Enter to continue or 'q' to quit "
        has_quit = input(quit_message)
        has_quit = len(has_quit) > 0 and 'q' in has_quit.lower()
        range_start = 0
        range_step = 10
        while not has_quit:
            print(table_df[range_start:range_start + range_step].to_string())
            range_start += range_step
            if range_start < len(table_df):
                has_quit = input(quit_message)
                has_quit = len(has_quit) > 0 and 'q' in has_quit.lower()
            else:
                has_quit = True


def _main(args, folder):
    args = parse(args)
    check_folders(folder)
    if args.fetch is not None:
        fetch(args.fetch, folder)
    elif args.new is not None:
        if args.new == "pipeline":
            new_pipeline(folder)
        elif args.new == "table":
            new_table(folder)
    elif args.view is not None:
        view(args.view, folder)


if __name__ == "__main__":
    argument_list = sys.argv[1:]
    file_folder = os.path.dirname(__file__)
    _main(argument_list, file_folder)
