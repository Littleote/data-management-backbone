# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""
import os
import sys
import argparse

ZONES = ["landing", "formatted", "trusted", "exploitation", "dataset_info"]

def check_folders(folder):
    directories = os.listdir(folder)
    for zone in ZONES:
        if zone not in directories:
            os.mkdir(zone)

def parse(args):
    argParser = argparse.ArgumentParser(
        description='Data management pipeline',
        epilog='by David Candela and Andreu Giménez')
    action = argParser.add_mutually_exclusive_group(required=True)
    action.add_argument("--fetch", nargs='?', const=-1, metavar="DATASET"
                        , help="Fetch the latest version of a dataset")
    action.add_argument("--new", action="store_true"
                        , help="Create a new dataset pipeline")
    return argParser.parse_args(args)

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
            assert 0 < pipeline and pipeline <= len(pipelines)
        except:
            print("Invalid index")
            return
        pipeline = pipelines[pipeline - 1]
    else:
        if not pipeline in pipelines:
            print(f"{pipeline} pipeline doesn't exist")
            return
    # landing(pipeline, folder)
    # formatted(pipeline, folder)
    # trusted(pipeline, folder)
    # exploitation(folder)
    
def new_pipeline(folder):
    pass

def _main(args, folder):
    args = parse(args)
    check_folders(folder)
    if args.fetch is not None:
        fetch(args.fetch, folder)
    elif args.new:
        new_pipeline(folder)
    

if __name__ == "__main__":
    argument_list = sys.argv[1:]
    folder = os.path.dirname(__file__)
    _main(argument_list, folder)
    
