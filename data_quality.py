# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import os
import json
import duckdb
import Levenshtein

trusted_path = 'trusted/'
info_path = 'dataset_info/'

def data_quality():
    target_list = os.listdir(info_path)
    target_list = [target_item[:-5] for target_item in target_list if target_item[-5:] == ".json"]
    print("Select the two datasets to match for the join with a data quality process:")
    for i, target_item in enumerate(target_list):
        print(f"{i + 1}.- {target_item}")
    dataset_1_t = input("Index for the first = ")
    dataset_2_t = input("Index for the second = ")

    failed = False
    try:
        dataset_1_t = int(dataset_1_t)
        dataset_2_t = int(dataset_2_t)
        assert (0 < dataset_1_t) and (dataset_1_t <= len(target_list)) and (0 < dataset_2_t) and (dataset_2_t <= len(target_list)) and dataset_1_t != dataset_2_t
        dataset_1 = target_list[dataset_1_t - 1]
        dataset_2 = target_list[dataset_2_t - 1]
    except:
        failed = True
    if failed:
        raise RuntimeError(f"datasets index must be two different numbers between 1 and {len(target_list)}, not {dataset_1_t} and {dataset_2_t}")

    print("Processing the keys of ",dataset_1," and ", dataset_2)

    #For datset 1 choose key
    print("Key of dataset 1 is:")
    with open(info_path + dataset_1 +".json", mode='r') as handler:
        dataset_1_info = json.load(handler)
    keys_1=dataset_1_info.get("keys", [])
    if len(keys_1)>1:
        for i, target_item in enumerate(keys_1):
            print(f"{i + 1}.- {target_item}")
        key_1_t = input("Index of the key = ")

        failed = False
        try:
            key_1_t = int(key_1_t)
            assert (0 < key_1_t) and (key_1_t <= len(keys_1))
            key_1 = keys_1[key_1_t - 1]
        except:
            failed = True
        if failed:
            raise RuntimeError(f"Key index must be a number between 1 and {len(keys_1)}, not {key_1_t}")

    else:
        key_1=keys_1[0]
        print(f"{key_1}")


    #For dataset 2 choose key
    print("Key of dataset 2 is:")
    with open(info_path + dataset_2 +".json", mode='r') as handler:
        dataset_2_info = json.load(handler)

    keys_2=dataset_2_info.get("keys", [])
    if len(keys_2)>1:
        for i, target_item in enumerate(keys_2):
            print(f"{i + 1}.- {target_item}")
        key_2_t = input("Index of the key = ")
        failed = False
        try:
            key_2_t = int(key_2_t)
            assert (0 < key_2_t) and (key_2_t <= len(keys_2))
            key_2 = keys_2[key_2_t - 1]
        except:
            failed = True
        if failed:
            raise RuntimeError(f"Key index must be a number between 1 and {len(keys_2)}, not {key_2_t}")


    else:
        key_2=keys_2[0]
        print(f"{key_2}")

    with duckdb.connect(database=trusted_path + 'database.db', read_only=False) as db:
        # Attach tables and extract information
        df_1=db.execute(f"SELECT {key_1} FROM {dataset_1}").df()
        df_2=db.execute(f"SELECT {key_2} FROM {dataset_2}").df()


    llista_1 =df_1[key_1].values.tolist()
    llista_2 =df_2[key_2].values.tolist()

    llista_1=set(llista_1)
    llista_1=list(llista_1)

    llista_2=set(llista_2)
    llista_2=list(llista_2)

    if len(llista_2)>len(llista_1):
        tmp_2=llista_2
        llista_2=llista_1
        llista_1=tmp_2

        tmp_2=key_2
        key_2=key_1
        key_1=tmp_2

        tmp_2=dataset_2
        dataset_2=dataset_1
        dataset_1=tmp_2


    response = []
    best_match_name ="empty"


    for name_to_find in llista_1:

        best_match=11
        for name_looking in llista_2:
            val_tmp=Levenshtein.distance(name_to_find,name_looking)
            if best_match>val_tmp:
                best_match=val_tmp
                best_match_name=name_looking

        row = {'sample_name':name_to_find,'actual_name':best_match_name, 'score':best_match}
        response.append(row)

    lowest_scores = {}

    # Iterate through the data
    for entry in response:
        actual_name = entry['actual_name']
        score = entry['score']

        # Check if actual_name is not in the dictionary or if the score is lower
        if actual_name not in lowest_scores or score < lowest_scores[actual_name]['score']:
            lowest_scores[actual_name] = entry

    # Extract the unique actual_names with the lowest scores
    result = list(lowest_scores.values())


    result = sorted(result, key=lambda x: x['score'])

    # Print the result
    for entry in result:
        print(entry)

    print(len(result),len(llista_2),len(llista_1))


    # Replace with your actual DuckDB connection parameters
    with duckdb.connect(database=trusted_path + 'database.db', read_only=False) as db:

        def update_strings(mapping_list, table_name, column_name):
            for mapping in mapping_list:
                sample_name = mapping['sample_name']
                actual_name = mapping['actual_name']

                # Use parameterized query to prevent SQL injection
                if(actual_name!="empty"):
                    query = f"UPDATE {table_name} SET {column_name} = ? WHERE {column_name} = ?"
                    #print(query,actual_name,sample_name)
                    db.execute(query,[actual_name, sample_name])

        update_strings(result, dataset_1, key_1)

        # Close the connection
        db.close()
