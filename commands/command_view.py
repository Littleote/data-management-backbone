# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import os
import duckdb

from utils import select

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
                