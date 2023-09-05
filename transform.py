import logging
from typing import List

import dask
import pandas as pd

from helpers import catchtime, Task, append_chunk_to_csv
from settings import DF_CHUNK_SIZE


def transform_input_files(tasks: List[Task]) -> None:
    """
    Transforms input files using multiple cores (where available).

    :param tasks: A list of Task objects representing the input files to be transformed.
    """
    with catchtime() as duration:
        results = dask.compute(*[transform(task) for task in tasks])
    logging.info({
        "message": f"Successfully outputted processed file(s)",
        "duration": duration(),
        "results": results,
    })


@dask.delayed
def transform(task: Task) -> (str, str):
    with catchtime() as duration:
        for index, chunk in enumerate(get_chunked_df(task)):
            strip_leading_and_trailing_whitespace_from_column_names(chunk)
            rename_cols(chunk)
            cast_numeric_types(chunk)
            normalise_timestamps(chunk, task)
            write_header = True if index == 0 else False
            append_chunk_to_csv(chunk, write_header, task.output_filepath)
    return task.output_filepath, duration()


def get_chunked_df(task):
    if task.input_data_format == "json":
        return pd.read_json(task.temp_file, chunksize=DF_CHUNK_SIZE, lines=True)
    else:
        return pd.read_csv(task.temp_file, chunksize=DF_CHUNK_SIZE)


def cast_numeric_types(chunk):
    chunk['variable'] = chunk['variable'].astype(int)
    chunk['value'] = chunk['value'].astype(float)


def normalise_timestamps(chunk, task):
    unit = 'ms' if task.input_data_format == 'json' else None
    chunk['time_stamp'] = pd.to_datetime(chunk['time_stamp'], utc=True, unit=unit)
    chunk['last_modified'] = pd.to_datetime(chunk['last_modified'], utc=True, unit=unit)


def strip_leading_and_trailing_whitespace_from_column_names(chunk):
    chunk.columns = chunk.columns.str.strip()


def rename_cols(chunk):
    column_rename_mapping = {
        "Last Modified utc": "last_modified",
        "Variable": "variable",
        "Naive_Timestamp": "time_stamp",
    }
    chunk.rename(columns=column_rename_mapping, inplace=True)
