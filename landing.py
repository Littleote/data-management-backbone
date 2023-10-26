# -*- coding: utf-8 -*-
"""
@authors: david candela & andreu giménez
"""

import os
import re
import shutil
from datetime import date
from zipfile import ZipFile
import requests

temporal_path = 'landing/temporal/'
persistent_path = 'landing/persistent/'

def request(url):
    try:
        response = requests.get(url, timeout=10)
    except Exception as e:
        print(f"Unable to connect to server (URL: {url})")
        raise RuntimeError() from e
    if not response.ok:
        print(f"Failed request to server (URL: {url})")
        raise RuntimeError()
    return response


def check_folders():
    if os.path.exists(temporal_path):
        shutil.rmtree(temporal_path)
    os.mkdir(temporal_path)
    if not os.path.exists(persistent_path):
        os.mkdir(persistent_path)


def save_request(response):
    disposition = response.headers.get('Content-Disposition', "")
    filename = re.findall("filename=(.+)", disposition)
    if len(filename) > 0:
        filename = filename[0]
    else:
        filename = "out"
    filename = filename.replace('"', '').replace("'", '')
    with open(temporal_path + filename, 'wb') as handler:
        for chunk in response.iter_content(chunk_size=128):
            handler.write(chunk)
    return filename


def unfold_file(instructions):
    for step, name in instructions:
        files = os.listdir(temporal_path)
        coincidences = [f for f in files if re.search("^" + name + "$", f) is not None]
        assert len(coincidences) > 0, f"Error while searching {name} to unfold"
        coincidence = coincidences[0]
        if step == "file":
            filename = coincidence
            break
        if step == "zip":
            with ZipFile(temporal_path + coincidence, 'r') as zip_object:
                # Extracting elements of zip folder in temporal landing
                zip_object.extractall(path=temporal_path)
    return filename


def move_to_persistent(filename, target):
    extension = filename.split('.')[-1]
    if not os.path.isdir(persistent_path + target):
        os.mkdir(persistent_path + target)
    new_filename = target + '_' + date.today().strftime("%Y_%m_%d") + '.' + extension
    if os.path.exists(os.path.join(persistent_path, target, new_filename)):
        print("A file for this dataset from this date already exists, overwrite?")
        update = input("Y/[N]\n")
        if len(update) > 0 and update.lower()[0] == 'y':
            os.replace(temporal_path + filename,
                      os.path.join(persistent_path, target, new_filename))
            print("overwritten")
    else:
        os.rename(temporal_path + filename,
                  os.path.join(persistent_path, target, new_filename))
    return new_filename


def to_landing(pipeline, target):
    response = request(pipeline['URL'])
    check_folders()
    filename = save_request(response)
    if "unfold" in pipeline.keys():
        filename = unfold_file(filename)
    return move_to_persistent(filename, target)


if __name__ == "__main__":
    import json
    pipelines = os.listdir("dataset_info")
    pipelines = [pipe[:-5] for pipe in pipelines if pipe[-5:] == '.json']
    for pipe in pipelines:
        with open(os.path.join("dataset_info", pipe + '.json'),
                  mode='r', encoding='utf-8') as pipe_handler:
            pipeline_info = json.load(pipe_handler)
        to_landing(pipeline_info, pipe)
    