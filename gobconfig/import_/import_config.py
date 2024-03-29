"""Mapping.

Read a mapping from a file
"""


import json
import os
from collections import defaultdict
from typing import Any, Optional

from gobconfig.exception import GOBConfigException

DATASET_DIR = os.path.join(os.path.dirname(__file__), "data/")

DatasetPathType = str
FilenameType = str
DatasetMappingType = dict[str, Any]
ResultType = defaultdict[str, defaultdict[str, dict[str, DatasetPathType]]]


def _add_file_to_mapping(result: ResultType, filepath: DatasetPathType) -> None:
    """Add dataset filepath to result."""
    try:
        mapping = get_mapping(filepath)
        catalogue = mapping["catalogue"]
        collection = mapping["entity"]
        application = mapping["source"]["application"]
    except (KeyError, json.decoder.JSONDecodeError):
        raise GOBConfigException(f"Dataset file {filepath} invalid")

    if application in result[catalogue][collection]:
        raise GOBConfigException(
            f"Have multiple import definitions for {catalogue} {collection} with " f"application {application}"
        )
    result[catalogue][collection][application] = filepath

    if mapping.get("default"):
        if "_default" in result[catalogue][collection]:
            raise GOBConfigException(f"Have multiple defaults for {catalogue} {collection}")
        result[catalogue][collection]["_default"] = filepath


def _build_dataset_locations_mapping() -> ResultType:
    """Build dataset locations mapping based on JSON Files present in DATASET_DIR.

    :return:
    """
    # Init 3-dimensional dict
    result: ResultType = defaultdict(lambda: defaultdict(dict))

    for file in os.listdir(DATASET_DIR):
        filepath = DATASET_DIR + file
        if os.path.isfile(filepath) and file.endswith(".json"):
            _add_file_to_mapping(result, filepath)
    return result


def get_dataset_file_location(catalogue: str, collection: str, application: Optional[str] = None) -> DatasetPathType:
    """Return the dataset file location for the given catalogue, collection, application combination.

    Application may be omitted when there is only one application available for given catalogue and collection.

    :param catalogue:
    :param collection:
    :param application:
    :return:
    """
    try:
        if not application:
            applications = dataset_locations_mapping[catalogue][collection]

            if "_default" in applications:
                # Check if default is set
                return applications["_default"]

            if len(applications.keys()) > 1:
                raise GOBConfigException(
                    f"Multiple applications found for catalogue, collection combination: "
                    f"{catalogue}, {collection}. Please specify the application."
                )

            return next(iter(applications.values()))
        else:
            return dataset_locations_mapping[catalogue][collection][application]
    except (KeyError, IndexError, StopIteration):
        raise GOBConfigException(
            f"No dataset found for catalogue, collection, application combination: "
            f"{catalogue}, {collection}, {application}"
        )


def get_query(filename: FilenameType) -> list[str]:
    """Get query contents from file.

    Return an array of lines to be compatible with inline query contents.
    Return an empty array when the query file cannot be found.

    :param filename:
    :return:
    """
    try:
        # Load query from file
        sql_file = os.path.join(DATASET_DIR, filename)
        with open(sql_file, "r", encoding="utf8") as sql_reader:
            return sql_reader.readlines()
    except FileNotFoundError:
        print(f"Query file not found {filename}")
        return []


def get_mapping(filepath: DatasetPathType) -> DatasetMappingType:
    """Read a mapping from a file.

    :param filepath: path of the file that contains the mapping
    :return: an object that contains the mapping
    """
    with open(filepath, encoding="utf8") as file:
        mapping: DatasetMappingType = json.load(file)

        source_filename = mapping.get("source", {}).get("application_config", {}).get("filename")

        # Set source path to absolute filepath if filename exists in source config
        if source_filename:
            mapping["source"]["application_config"]["filepath"] = get_absolute_filepath(source_filename)

        # Test if query has to be read from file
        query = mapping.get("source", {}).get("query")
        if isinstance(query, str):
            mapping["source"]["query"] = get_query(filename=query)

        return mapping


def get_absolute_filepath(filename: FilenameType) -> DatasetPathType:
    """Return absolute filepath for filename. filename should be relative to the data directory.

    :param filename:
    :return:
    """
    return os.path.join(DATASET_DIR, filename)


def get_import_definition_by_filename(filename: FilenameType) -> DatasetMappingType:
    """Return import definition with filename relative to data directory.

    :param filename:
    :return:
    """
    return get_mapping(get_absolute_filepath(filename))


def get_import_definition(catalogue: str, collection: str, application: Optional[str] = None) -> DatasetMappingType:
    """Return import definition for the given catalogue, collection, application combination."""
    file = get_dataset_file_location(catalogue, collection, application)
    return get_mapping(file)


dataset_locations_mapping = _build_dataset_locations_mapping()
