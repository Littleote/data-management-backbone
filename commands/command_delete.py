# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import os
import json

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

    # Select options to delete
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
        print(f"Do you really want to delete '{sep.join(selected)}'")
        if not confirm(None, default=False):
            selected = []
    if len(selected) <= 0:
        return

    if kind == "pipeline":
        for option in selected:
            os.remove(os.path.join(folder, "dataset_info", option))
    elif kind == "table":
        for option in selected:
            data.pop(option)
        with open(file_path, mode='w', encoding='utf-8') as file:
            json.dump(data, file, indent=4)
            