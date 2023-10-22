# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import os
import sys
import json
import argparse

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


def parse(args):
    arg_parser = argparse.ArgumentParser(
        description='Data management pipeline',
        epilog='by David Candela and Andreu Giménez')
    action = arg_parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--fetch", nargs='?', const=-1, metavar="DATASET"
                        , help="Fetch the latest version of a dataset")
    action.add_argument("--new", choices=["pipeline", "table"]
                        , help="Create a new dataset pipeline or a new exploitation table")
    return arg_parser.parse_args(args)


def fetch(pipeline, folder):
    pipelines = os.listdir(os.path.join(folder, "dataset_info"))
    pipelines = [pipe[:-5] for pipe in pipelines if pipe[-5:] == '.json']
    if pipeline == -1:
        print("Available pipelines:")
        for i, pipe in enumerate(pipelines):
            print(f"   {i + 1}.- {pipe}")
        print()
        pipeline = input("Select a pipeline's index: ")
        try:
            pipeline = int(pipeline)
            assert 0 < pipeline <= len(pipelines)
        except (AssertionError, ValueError):
            print("Invalid index")
            return
        pipeline = pipelines[pipeline - 1]
    else:
        if not pipeline in pipelines:
            print(f"{pipeline} pipeline doesn't exist")
            return
    working_directory = os.getcwd()
    os.chdir(folder)
    with open(os.path.join(folder, "dataset_info", pipeline + '.json'),
              mode='r', encoding='utf-8') as handler:
        pipeline_info = json.load(handler)
    landing.to_landing(pipeline_info, pipeline)
    formatted.to_formatted(pipeline_info, pipeline)
    trusted.to_trusted(pipeline_info, pipeline)
    # trusted.clean_data(pipeline_info, pipeline)
    exploitation.to_exploitation()
    os.chdir(working_directory)


def new_pipeline(folder):
    pass


def new_table(folder):
    pass


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


if __name__ == "__main__":
    argument_list = sys.argv[1:]
    file_folder = os.path.dirname(__file__)
    _main(argument_list, file_folder)
