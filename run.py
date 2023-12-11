# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import os
import sys
import argparse

from commands import fetch, new, update, delete, view

from utils import Path, get_zones


def check_folders(folder):
    with Path(folder):
        for zone in get_zones():
            os.makedirs(zone, exist_ok=True)


def parse(args):
    arg_parser = argparse.ArgumentParser(
        description='Data management pipeline',
        epilog='by David Candela and Andreu Giménez')
    action = arg_parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--fetch", nargs='?', const=-1, metavar="DATASET"
                        , help="Fetch the latest version of a dataset")
    action.add_argument("--new", type = str.lower,
                        choices=["pipeline", "table", "analysis", "dataset", "model"]
                        , help="""Create one of the following:
a new dataset PIPELINE,
a new exploitation TABLE,
a new set of tables in sandbox for an ANALYSIS,
a new transformation of sandbox tables into a DATASET,
a new MODEL to perform an analysis""")
    action.add_argument("--update", type = str.lower,
                        choices=["table", "analysis", "dataset", "model"]
                        , help="Update the parameters of the given object and rerun it")
    action.add_argument("--delete", type = str.lower, choices=["pipeline", "table"]
                        , help="Delete a dataset pipeline or an exploitation table query")
    action.add_argument("--view", choices=["formatted", "trusted", "exploitation"]
                        , metavar="ZONE", type = str.lower
                        , help="Visualize a table in the specified zone \n" \
                            "(formatted, trusted or exploitation)")
    return arg_parser.parse_args(args)


def _main(args, folder):
    args = parse(args)
    check_folders(folder)
    if args.fetch is not None:
        fetch(args.fetch, folder)
    elif args.new is not None:
        new(args.new, folder)
    elif args.update is not None:
        update(args.update, folder)
    elif args.delete is not None:
        delete(args.delete, folder)
    elif args.view is not None:
        view(args.view, folder)


if __name__ == "__main__":
    argument_list = sys.argv[1:]
    file_folder = os.path.dirname(__file__)
    _main(argument_list, file_folder)
