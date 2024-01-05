# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import os
import json

import trusted
import data_quality
import exploitation

from utils import Path

def regenerate(kind, folder):
    if kind == "pipeline":
        regenerate_pipeline(folder)
    else:
        raise NotImplementedError(f"new {kind} not yet implemented")


def regenerate_pipeline(folder):
    with Path(folder):
        # Load Pipeline names
        pipelines = []
        pipelines_info = []
        for pipe in os.listdir(os.path.join(folder, "dataset_info")):
            if pipe[-5:] != '.json':
                continue
            pipelines.append(pipe[:-5])
            # Load JSON parameters
            with open(os.path.join("dataset_info", pipe), mode='r', encoding='utf-8') as handler:
                pipelines_info.append(json.load(handler))

        with Path(folder):
            try:
                # Run pipeline
                padded = "{:<40s}".format
                print(padded("Joining dataset versions"), end="\r")
                list(map(trusted.to_trusted, pipelines_info, pipelines))
                print(padded("Cleaning dataset"), end="\r")
                list(map(trusted.clean_data, pipelines_info, pipelines))
                print(padded("Performing data quality checks"), end="\r")
                list(map(data_quality.perform_pipeline_matches, pipelines))
                print(padded("Generaiting exploitation tables"), end="\r")
                exploitation.to_exploitation()
                print(padded("Done"), end="\n")
            except RuntimeError as err:
                print("\nOperation aborted")
                print(err.args)
            