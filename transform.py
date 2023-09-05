import logging
import os
from typing import List, Union

import dask
import pandas as pd
from pandera.errors import SchemaError

from helpers import catchtime, Task, append_chunk_to_csv
from settings import DF_CHUNK_SIZE


def transform_input_files(tasks: List[Task]) -> None:
    """
    Transforms input files using multiple cores (where available).

    :param tasks: A list of Task objects representing the input files to be transformed.
    """
    with catchtime() as duration:
        results = dask.compute(*[attempt_transformation(task) for task in tasks])
    logging.info({
        "message": f"Successfully outputted processed file(s)",
        "duration": duration(),
        "results": results,
    })


def transform(task: Task):
    success = False
    with catchtime() as duration:
        for index, chunk in enumerate(get_chunked_df(task)):
            try:
                task.data_schema.validate(chunk)
                strip_leading_and_trailing_whitespace_from_column_names(chunk)
                rename_cols(chunk)
                normalise_timestamps(chunk, unit=get_unit(task))
                write_header = True if index == 0 else False
                append_chunk_to_csv(chunk, write_header, task.output_filepath)
                success = True
            except SchemaError as e:
                logging.error({"message": "Schema Error", "file": task.temp_file})
                return (task.output_filepath, None, success)
            except Exception as e:
                logging.error({"message": "Uncaught Error", "file": task.temp_file})
                return (task.output_filepath, None, success)
    return task.output_filepath, duration(), success


def get_unit(task: Task) -> Union['ms', None]:
    """'ms' for epoch (found in json) otherwise None for csv timestamps."""
    return 'ms' if task.input_data_format == 'json' else None


@dask.delayed
def attempt_transformation(task: Task) -> (str, str):
    result = ("Failed", None)  # status, duration
    if os.path.exists(task.temp_file):
        result = transform(task)
    else:
        logging.info({"message": "Input file not found", "file": task.temp_file})
    return result


def get_chunked_df(task: Task) -> pd.DataFrame:
    if task.input_data_format == "json":
        return pd.read_json(task.temp_file, chunksize=DF_CHUNK_SIZE, lines=True)
    else:
        return pd.read_csv(task.temp_file, chunksize=DF_CHUNK_SIZE)


def normalise_timestamps(chunk: pd.DataFrame, unit):
    chunk['time_stamp'] = pd.to_datetime(chunk['time_stamp'], utc=True, unit=unit)
    chunk['last_modified'] = pd.to_datetime(chunk['last_modified'], utc=True, unit=unit)


def strip_leading_and_trailing_whitespace_from_column_names(chunk: pd.DataFrame):
    chunk.columns = chunk.columns.str.strip()


def rename_cols(chunk: pd.DataFrame):
    column_rename_mapping = {
        "Last Modified utc": "last_modified",
        "Variable": "variable",
        "Naive_Timestamp": "time_stamp",
    }
    chunk.rename(columns=column_rename_mapping, inplace=True)
