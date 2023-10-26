# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import os
import sys
import json
import argparse
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
            os.mkdir(zone)


def select(name, options):
    if len(options) > 1:
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
        print(f"Only {name} available is {options[0]}, procced?")
        option = input("[Y]/N\n")
        if len(option) <= 0 or option.lower()[0] != 'n':
            return options[0]
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
                        , help="Visualize a table in the specified zone \n (formatted, trusted or exploitation)")
    return arg_parser.parse_args(args)


def fetch(pipeline, folder):
    pipelines = os.listdir(os.path.join(folder, "dataset_info"))
    pipelines = [pipe[:-5] for pipe in pipelines if pipe[-5:] == '.json']
    if pipeline == -1:
        pipeline = select("pipeline", pipelines)
        if pipeline is None:
            return
    else:
        if not pipeline in pipelines:
            print(f"{pipeline} pipeline doesn't exist")
            return
    working_directory = os.getcwd()
    os.chdir(folder)
    with open(os.path.join(folder, "dataset_info", pipeline + '.json'),
              mode='r', encoding='utf-8') as handler:
        pipeline_info = json.load(handler)
    try:
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
    pass


def new_table(folder):
    pass


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
