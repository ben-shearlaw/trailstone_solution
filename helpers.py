import asyncio
from contextlib import contextmanager
from datetime import datetime, timedelta
from functools import lru_cache
import logging
import os
from time import perf_counter
from typing import List, Literal
from urllib.parse import urljoin

import aiofiles.os
import pandas as pd
from pydantic import BaseModel, HttpUrl, NewPath
import pandera as pa

from settings import LOGS_DIR, OUTPUT_DIR, RUN_ID, TEMP_DIR


class Task(BaseModel):
    input_data_url: HttpUrl
    input_data_format: Literal['json', 'csv']
    output_filepath: NewPath
    temp_file: NewPath
    schema: object


def get_latest_dates() -> List[str]:
    dates = []
    current_date = datetime.now()
    start_date = current_date - timedelta(days=7)
    while start_date <= current_date:
        dates.append(start_date.strftime('%Y-%m-%d'))
        start_date += timedelta(days=1)
    return dates


async def create_necessary_dirs_if_needed():
    for dir in (LOGS_DIR, OUTPUT_DIR):
        if not await aiofiles.os.path.exists(dir):
            await aiofiles.os.makedirs(dir)


def validate_tasks(tasks_raw: List[dict]) -> List[Task]:
    """
    Validates a list of unvalidated ('raw') tasks and returns a list of validated tasks.
    """
    validated_tasks = []
    for task in tasks_raw:
        validated_tasks.append(Task(**task))
    logging.info("Task configuration(s) are valid")
    return validated_tasks


@contextmanager
def catchtime() -> str:
    """Return in milliseconds, how long the context took to execute."""
    start = perf_counter()
    yield lambda: f"{(perf_counter() - start) * 1000:.0f}ms"


@lru_cache()
def get_csv_schema():
    return pa.DataFrameSchema({
        "Naive_Timestamp ": pa.Column(pa.String, nullable=False),
        " Variable": pa.Column(pa.Int, nullable=False),
        "value": pa.Column(pa.Float, nullable=False),
        "Last Modified utc": pa.Column(pa.String, nullable=False),
    })


@lru_cache()
def get_json_schema():
    return pa.DataFrameSchema({
        "Naive_Timestamp ": pa.Column(pa.Int, nullable=False),
        " Variable": pa.Column(pa.Int, nullable=False),
        "value": pa.Column(pa.Float, nullable=False),
        "Last Modified utc": pa.Column(pa.Int, nullable=False),
    })


def construct_raw_tasks_list(dates: List[str]) -> List[dict]:
    tasks_raw = []
    json_schema, csv_schema = get_json_schema(), get_csv_schema()
    for date in dates:
        wind_url_path = f"/{date}/renewables/windgen.csv"
        solar_url_path = f"/{date}/renewables/solargen.json"
        tasks_raw.extend([
            {
                "input_data_url": urljoin(os.getenv("API_HOST"), solar_url_path),
                "input_data_format": "json",
                "output_filepath": os.path.join(OUTPUT_DIR, f"{RUN_ID}_{date}_solar.csv"),
                "temp_file": os.path.join(TEMP_DIR, f"{RUN_ID}_{date}_solar.jsonl"),
                "schema": json_schema,
            },
            {
                "input_data_url": urljoin(os.getenv("API_HOST"), wind_url_path),
                "input_data_format": "csv",
                "output_filepath": os.path.join(OUTPUT_DIR, f"{RUN_ID}_{date}_wind.csv"),
                "temp_file": os.path.join(TEMP_DIR, f"{RUN_ID}_{date}_wind.csv"),
                "schema": csv_schema,
            },
        ])
    return tasks_raw


def append_chunk_to_csv(chunk: pd.DataFrame, write_header: bool, output_filepath: str):
    pd.DataFrame.to_csv(chunk, output_filepath, index=False, mode='a', header=write_header)


def assemble_list_of_tasks() -> List[Task]:
    dates = get_latest_dates()
    tasks_raw = construct_raw_tasks_list(dates)
    tasks = validate_tasks(tasks_raw)
    return tasks


async def remove_temp_file(filepath):
    await aiofiles.os.remove(filepath)


async def cleanup_temp_files(tasks: List[Task]):
    jobs = []
    for task in tasks:
        jobs.append(asyncio.ensure_future(remove_temp_file(task.temp_file)))
    with catchtime() as duration:
        await asyncio.gather(*jobs, return_exceptions=True)
    logging.info({"message": f"Removed temp file(s)", "duration": duration()})
