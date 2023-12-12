# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import os
import json
import duckdb

import exploitation
import sandbox

from utils import Path, confirm, select, get_zones

DB_FILE = 'database.db'


def update(kind, folder):
    if kind == "table":
        update_table(folder)
    elif kind == "analysis":
        update_analysis(folder)
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
    if not confirm(None):
        print("Update canceled")
        return
    data[name_table] = sql_code

    # Open file and write the data with the new table
    with open(file_path, mode='w', encoding='utf-8') as file:
        json.dump(data, file, indent=4)

    # Rerun exploitation
    exploitation.merge_tables({name_table: sql_code})    

    # Confirmation
    print("Your table has been succesfully updated")


def update_analysis(folder):
    with Path(folder):
        # Get list of analysis in use (with models)
        try:
            with duckdb.connect(f"models/{DB_FILE}", read_only=False) as con:
                used = con.execute("SELECT DISTINCT analysis FROM Models").df()
                used = list(used["analysis"])
        except duckdb.CatalogException:
            used = []
        
        # Open file and read data
        if os.path.isfile("sandbox/analysis.json"):
            with open("sandbox/analysis.json", mode='r', encoding='utf-8') as handler:
                analysis = json.load(handler)
        else:
            print("No analysis present to update")
            return

        # Introduce the new name
        name = input("Insert the name of the analysis to update: ")
        if name not in analysis.keys():
            print("This name is not in use, insert a name in:")
            print(*list(analysis.keys()), sep=',')
            name = input("Name of the analysis to update: ")
            if name not in analysis.keys():
                print("Name not present")
                return

        if name in used:
            raise NotImplementedError("Not implemented analysis duplication")

        info = analysis[name]

        if confirm("Update analysis parameters?", force_confirmation=True):
            analysis_type = select("analysis type", ["regression", "categorical", "binary"])
            if analysis_type is None:
                print("Update analysis aborted")
                return
            info["type"] = analysis_type
    
            # Get possible columns to use
            with duckdb.connect(f"exploitation/{DB_FILE}", read_only=True) as con:
                tables = con.execute("SHOW ALL TABLES").df()
            for _, row in tables.iterrows():
                if confirm(f"Use table {row['name']} for the analysis?"):
                    no_opt = "<CONTINUE>"
                    all_opt = "<ALL>"
                    special_opt = [no_opt, all_opt]
    
                    # Column select
                    col_names = row["column_names"]
                    selection = []
                    variable = ""
                    options = special_opt + col_names
                    while variable not in special_opt:
                        variable = select("column", options)
                        if variable is not None:
                            options.remove(variable)
                            selection.append(variable)
                    if variable == all_opt:
                        selection = '*'
                    elif variable == no_opt:
                        selection.remove(no_opt)
                    info['data'][row['name']] = selection
    
                    # Constraint addition
                    if confirm(f"Use only a subset of table {row['name']} for the analysis?",
                               default=False):
                        sql_constraint = input(
                            "Now write the subset selection code (SQL WHERE clause): ")
                        info['constraints'][row['name']] = sql_constraint
            analysis[name] = info
    
            # Open file and write the data with the new table
            with open("sandbox/analysis.json", mode='w', encoding='utf-8') as handler:
                json.dump(analysis, handler, indent=4)

            # Confirmation
            print("The parameters have been succesfully updated")

        # Fetch analysis tables
        sandbox.fetch_analysis_data(name)

        # Confirmation
        print("The analysis tables have been updated")

