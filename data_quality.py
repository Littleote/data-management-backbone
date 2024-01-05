# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import os
import json
import duckdb
import Levenshtein

from utils import select

trusted_path = 'trusted/'
info_path = 'dataset_info/'

THRESHOLD_DISTANCE = 11
MATCH_TYPE = ["VARCHAR"]


def get_match_list(func=None):
    try:
        with open(trusted_path + "data_quality.json", mode='r', encoding='utf-8') as handler:
            matches = json.load(handler)
            if func is not None:
                matches = list(filter(func, matches))
    except FileNotFoundError:
        matches = []
    return matches


def set_match_list(matches):
    with open(trusted_path + "data_quality.json", mode='w', encoding='utf-8') as handler:
        json.dump(matches, handler, indent=4)


def define_new_match():
    pipes = os.listdir(info_path)
    pipes = [pipe[:-5] for pipe in pipes if pipe[-5:] == ".json"]
    with duckdb.connect(database=trusted_path + 'database.db', read_only=False) as con:
        tables = con.execute("SHOW ALL TABLES").df()
        populated = list(tables["name"])
    target_list = [target for target in pipes if target in populated]

    print("Select the two datasets to match for the join with a data quality process:")
    datasets = []
    for i in range(2):
        datasets.append(select("dataset", target_list))
        if datasets[-1] is None:
            raise RuntimeError("No dataset selected")
        target_list.remove(datasets[-1])

    print(f"Select the columns to match between {datasets[0]} and {datasets[1]}")
    match_rule = {}
    for i, dataset in enumerate(datasets):
        print(f"Columns of dataset {i + 1} ({dataset}):")
        table = tables.loc[tables["name"] == dataset].iloc[0]
        names = table["column_names"]
        types = table["column_types"]
        columns = [n for n, t in zip(names, types) if t in MATCH_TYPE]
        column = select("column", columns)
        if column is None:
            raise RuntimeError("No column selected")
        match_rule[dataset] = column

    matches = get_match_list()
    matches.append(match_rule)
    set_match_list(matches)


def delete_pipeline_matches(pipeline):
    matches = get_match_list(lambda match: pipeline not in match.keys())
    set_match_list(matches)


def perform_pipeline_matches(pipeline):
    matches = get_match_list(lambda match: pipeline in match.keys())
    for match in matches:
        try:
            perform_match(match)
        except (duckdb.CatalogException, duckdb.ParserException) as err:
            datasets = list(match.keys())
            print(f"Unable to macth table {datasets[0]} with {datasets[1]}")
            print("Causes:")
            print(*err.args, sep="\n")


def perform_match(pair):
    dataset_1, dataset_2 = pair.keys()
    key_1, key_2 = pair[dataset_1], pair[dataset_2]
    with duckdb.connect(database=trusted_path + 'database.db', read_only=False) as con:
        # Attach tables and extract information
        df_1 = con.execute(f"SELECT DISTINCT {key_1} FROM {dataset_1}").df()
        df_2 = con.execute(f"SELECT DISTINCT {key_2} FROM {dataset_2}").df()

    llista_1 = list(df_1[key_1])
    llista_2 = list(df_2[key_2])

    if len(llista_2) > len(llista_1):
        llista_1, llista_2 = llista_2, llista_1
        key_1, key_2 = key_2, key_1
        dataset_1, dataset_2 = dataset_2, dataset_1

    mapping_dict = {}
    no_mapping = {"score": THRESHOLD_DISTANCE}

    for name_to_find in llista_1:
        best_score = THRESHOLD_DISTANCE
        best_match = None
        for name_looking in llista_2:
            score = Levenshtein.distance(name_to_find, name_looking)
            if best_score > score:
                best_score = score
                best_match = name_looking
        mapping = mapping_dict.get(best_match, no_mapping)
        if mapping["score"] > best_score:
            mapping_dict[best_match] = {
                'sample_name': name_to_find,
                'actual_name': best_match,
                'score': best_score
            }

    # Extract the unique actual_names with the lowest scores
    mapping_list = list(mapping_dict.values())

    # Connect to database
    with duckdb.connect(database=trusted_path + 'database.db', read_only=False) as con:
        for mapping in mapping_list:
            sample_name = mapping['sample_name']
            actual_name = mapping['actual_name']

            # Use parameterized query to prevent SQL injection
            if actual_name is not None:
                query = f"UPDATE {dataset_1} SET {key_1} = ? WHERE {key_1} = ?"
                con.execute(query, [actual_name, sample_name])
