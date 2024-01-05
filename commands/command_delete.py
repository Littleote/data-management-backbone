# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import os
import json

import data_quality

from utils import confirm, select

def delete(kind, folder):
    no_opt = "<DONE>"

    # Load delete options
    if kind == "pipeline":
        options = os.listdir(os.path.join(folder, "dataset_info"))
        options = [pipe for pipe in options if pipe[-5:] == '.json']
    elif kind == "table":
        file_path = os.path.join(folder, "exploitation", "tables.json")
        with open(file_path, mode='r', encoding='utf-8') as file:
            data = json.load(file)
        options = list(data.keys())
    elif kind == "match":
        options = data_quality.get_match_list()

    # Select options to delete
    if len(options) <= 0:
        print(f"No {kind} to delete")
        return
    selected = []
    option = ""
    options = [no_opt] + options
    while option != no_opt:
        option = select(kind, options)
        if option is not None and option != no_opt:
            selected += [option]
            options.remove(option)

    # Confirm and delete
    if len(selected) > 0:
        sep = "', '"
        print(f"Do you really want to delete '{sep.join(map(str, selected))}'")
        if not confirm(None, default=False):
            selected = []
    if len(selected) <= 0:
        return

    if kind == "pipeline":
        for option in selected:
            data_quality.delete_pipeline_matches(option)
            os.remove(os.path.join(folder, "dataset_info", option))
    elif kind == "table":
        for option in selected:
            data.pop(option)
        with open(file_path, mode='w', encoding='utf-8') as file:
            json.dump(data, file, indent=4)
    elif kind == "match":
        matches = list(filter(lambda e: isinstance(e, dict), options))
        data_quality.set_match_list(matches)
            