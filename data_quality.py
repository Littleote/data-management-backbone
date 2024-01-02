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

def define_new_match():
    target_list = os.listdir(info_path)
    target_list = [target_item[:-5] for target_item in target_list if target_item[-5:] == ".json"]
    print("Select the two datasets to match for the join with a data quality process:")
    dataset_1 = select("dataset", target_list)
    if dataset_1 is None:
        raise RuntimeError("No dataset selected")
    target_list.remove(dataset_1)
    dataset_2 = select("dataset", target_list)
    if dataset_2 is None:
        raise RuntimeError("No dataset selected")

    print(f"Processing the keys of {dataset_1} and {dataset_2}")

    #For datset 1 choose key
    print("Key of dataset 1:")
    with open(info_path + dataset_1 + ".json", mode='r', encoding='utf-8') as handler:
        dataset_1_info = json.load(handler)
    keys_1 = dataset_1_info.get("keys", [])
    key_1 = select("key", keys_1)
    if key_1 is None:
        raise RuntimeError("No key selected")

    #For datset 2 choose key
    print("Key of dataset 2:")
    with open(info_path + dataset_2 + ".json", mode='r', encoding='utf-8') as handler:
        dataset_2_info = json.load(handler)
    keys_2 = dataset_2_info.get("keys", [])
    key_2 = select("key", keys_2)
    if key_2 is None:
        raise RuntimeError("No key selected")
    return {dataset_1: key_1, dataset_2: key_2}


def check_quality(pipeline):
    try:
        with open(trusted_path + "data_quality.json", mode='r', encoding='utf-8') as handler:
            matches = json.load(handler)
            matches = [elem for elem in matches if pipeline in elem.keys()]
    except FileNotFoundError:
        matches = []
    for elem in matches:
        try:
            match(elem)
        except (duckdb.CatalogException, duckdb.ParserException) as err:
            datasets = list(elem.keys())
            print(f"Unable to macth table {datasets[0]} with {datasets[1]}")
            print("Causes:")
            print(*err.args, sep="\n")


def match(pair):
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
    mapping_list = sorted(mapping_list, key=lambda x: x['score'])

    # Print the result
    for entry in mapping_list:
        print(entry)
    print(len(mapping_list), len(llista_2), len(llista_1))

    # Connect to database
    with duckdb.connect(database=trusted_path + 'database.db', read_only=False) as con:
        for mapping in mapping_list:
            sample_name = mapping['sample_name']
            actual_name = mapping['actual_name']

            # Use parameterized query to prevent SQL injection
            if actual_name is not None:
                query = f"UPDATE {dataset_1} SET {key_1} = ? WHERE {key_1} = ?"
                con.execute(query, [actual_name, sample_name])
