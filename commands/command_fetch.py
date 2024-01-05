# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import os
import json

import landing
import formatted
import trusted
import data_quality
import exploitation

from utils import Path, select

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

    with Path(folder):
        # Load JSON parameters
        with open(os.path.join(folder, "dataset_info", pipeline + '.json'),
                  mode='r', encoding='utf-8') as handler:
            pipeline_info = json.load(handler)

        try:
            # Run pipeline
            padded = "{:<40s}".format
            print(padded("Fetching dataset"), end="\r")
            landing.to_landing(pipeline_info, pipeline)
            print(padded("Formatting dataset"), end="\r")
            formatted.to_formatted(pipeline_info, pipeline)
            print(padded("Joining dataset versions"), end="\r")
            trusted.to_trusted(pipeline_info, pipeline)
            print(padded("Cleaning dataset"), end="\r")
            trusted.clean_data(pipeline_info, pipeline)
            print(padded("Performing data quality checks"), end="\r")
            data_quality.perform_pipeline_matches(pipeline)
            print(padded("Generaiting exploitation tables"), end="\r")
            exploitation.to_exploitation()
            print(padded("Done"), end="\n")
        except RuntimeError as err:
            print("\nOperation aborted")
            print(err.args)
            