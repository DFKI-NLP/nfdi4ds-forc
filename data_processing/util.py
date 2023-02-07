from typing import Any
import json
import pandas as pd
import os

def recursive_items(dictionary: Any) -> str:
    """
    Generator that yields values in nested dictionary or List structure.
    Iterates through whole structure.

    Parameters
    ----------
    dictionary: Any
        nested Dict or List

    Returns
    -------
    str
    """
    if type(dictionary) is dict:
        for key, value in dictionary.items():
            if type(value) is dict or type(value) is list:
                yield from recursive_items(value)
            else:
                yield value
    if type(dictionary) is list:
        for value in dictionary:
            yield from recursive_items(value)


def create_json(df: pd.DataFrame, path: str) -> None:
    """
    Creates json file from dataframe.

    Parameters
    ----------
    df : pd.DataFrame
    path : str
        relative or absolute path for folder

    Returns
    -------
    """
    json_data = df.to_json(orient="index")
    with open(path, 'w') as outfile:
        json.dump(json_data, outfile)


def create_csv(df: pd.DataFrame, path: str) -> None:
    """
    Parameters
    ----------
    df : pd.DataFrame
    path :
        relative or absolute path for folder

    Returns
    -------
    """
    df.to_csv(path, index=False)


def delete_files(path: str, extension: str):
    """
    Deletes all files in with specific extension in folder.

    Parameters
    ----------
    path:
        path to folder
    extension:
        file extension

    Returns
    -------
    """
    dir_name = os.path.join(path)
    files = os.listdir(dir_name)

    for item in files:
        if item.endswith(extension):
            os.remove(os.path.join(dir_name, item))



