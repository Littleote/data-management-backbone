# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import os
import json

import exploitation

from utils import Path, select, get_zones


def update(kind, folder):
    if kind == "table":
        update_table(folder)
    else:
        raise NotImplementedError(f"update {kind} not yet implemented")


def check_folders(folder):
    with Path(folder):
        for zone in get_zones():
            os.makedirs(zone, exist_ok=True)


def update_table(folder):
    # Define json path
    file_path = os.path.join(folder, "exploitation", "tables.json")

    # Open file and write the data with the new table
    with open(file_path, mode='r', encoding='utf-8') as handler:
        data = json.load(handler)

    # Introduce the new table and SQL code
    name_table = select("table", list(data.keys()))
    if name_table is None:
        print("Update canceled")
        return
    sql_code = input("Now insert the new SQL code needed to create the table: ")

    # Confirm updated query
    print("Change from OLD:")
    print(data[name_table])
    print("to NEW:")
    print(sql_code)
    option = input("[Y]/N\n")
    if len(option) > 0 and option.lower()[0] == 'n':
        print("Update canceled")
        return
    data[name_table] = sql_code

    # Open file and write the data with the new table
    with open(file_path, mode='w', encoding='utf-8') as file:
        json.dump(data, file, indent=4)

    # Rerun exploitation
    exploitation.merge_tables({name_table: sql_code})    

    # Confirmation
    print("Your new table has been succesfully updated")
