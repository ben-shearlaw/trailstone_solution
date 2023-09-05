import asyncio
from contextlib import contextmanager
import os
import tempfile
import logging
import logging.config
from time import perf_counter
from urllib.parse import urljoin
import uuid
import contextvars
from typing import Literal, List
from datetime import datetime, timedelta

import simplejson as json
import aiofiles.os
import dask
import aiofiles
from dotenv import load_dotenv
from httpx import AsyncClient
import pandas as pd
import ijson
from pydantic import BaseModel, HttpUrl, NewPath
import tenacity

load_dotenv()
requested_date = "2023-01-01"
output_dir = "output"
logs_dir = "logs"
temp_dir = tempfile.gettempdir()
run_id_context_var = contextvars.ContextVar("run_id", default=str(uuid.uuid4())[:8])
run_id = run_id_context_var.get()


class Task(BaseModel):
    input_data_url: HttpUrl
    input_data_format: Literal['json', 'csv']
    output_filepath: NewPath
    temp_file: NewPath


class IDFilter(logging.Filter):
    def filter(self, record):
        record.run_id = run_id
        return True


def configure_logging() -> None:
    """
    This method configures the logging system with a custom configuration.
    It sets up handlers to log messages to both the console and a '.jsonl' formatted file.
    """
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'custom': {
                '()': 'pythonjsonlogger.jsonlogger.JsonFormatter',
                'format': '%(asctime)s %(levelname)s:%(message)s',
                'datefmt': '%d-%m-%Y %H:%M:%S'
            }
        },
        'filters': {
            'run_id': {
                '()': IDFilter,
            }},
        'handlers': {
            'stream': {
                'class': 'logging.StreamHandler',
                'formatter': 'custom',
                'filters': ['run_id'],
            },
            'file': {
                'class': 'logging.FileHandler',
                'filename': os.path.join(logs_dir, f'{run_id}.jsonl'),
                'formatter': 'custom',
                'filters': ['run_id'],
            }
        },
        'loggers': {
            '': {  # root logger
                'handlers': ['stream', 'file'],
                'level': 'INFO',
            }
        }
    })


def get_latest_dates() -> List[str]:
    dates = []
    current_date = datetime.now()
    start_date = current_date - timedelta(days=7)
    while start_date <= current_date:
        dates.append(start_date.strftime('%Y-%m-%d'))
        start_date += timedelta(days=1)
    return dates


def construct_tasks_list(dates):
    tasks_raw = []
    for date in dates:
        wind_url_path = f"/{date}/renewables/windgen.csv"
        solar_url_path = f"/{date}/renewables/solargen.json"
        tasks_raw.extend([
            {
                "input_data_url": urljoin(os.getenv("API_HOST"), solar_url_path),
                "input_data_format": "json",
                "output_filepath": os.path.join(output_dir, f"{run_id}_{date}_solar.csv"),
                "temp_file": os.path.join(temp_dir, f"{run_id}_{date}_solar.jsonl"),
            },
            {
                "input_data_url": urljoin(os.getenv("API_HOST"), wind_url_path),
                "input_data_format": "csv",
                "output_filepath": os.path.join(output_dir, f"{run_id}_{date}_wind.csv"),
                "temp_file": os.path.join(temp_dir, f"{run_id}_{date}_wind.csv"),
            },
        ])
    return tasks_raw


async def convert_json_file_to_jsonl(task):
    async with aiofiles.open(task.temp_file, 'r') as json_file:
        async with aiofiles.open(f"{task.temp_file}_reformatted", mode='w') as jsonl_file:
            async for record in ijson.items_async(json_file, "item"):
                await jsonl_file.write(json.dumps(record) + "\n")
    await aiofiles.os.rename(f"{task.temp_file}_reformatted", task.temp_file)


async def convert_all_json_files_to_jsonl(tasks):
    """This is done so that the data can be chunked by pandas."""
    with catchtime() as duration:
        conversion_jobs = []
        for task in tasks:
            if task.input_data_format == "json":
                conversion_jobs.append(asyncio.ensure_future(convert_json_file_to_jsonl(task)))
        await asyncio.gather(*conversion_jobs)
    logging.info({"message": "converted JSON to JSONL", "duration": duration()})


async def run_etl_pipeline() -> None:
    """
    Runs the ETL (Extract, Transform, Load) pipeline for the given tasks.

    Args:
        tasks_raw (List[dict]): A list of tasks describing the data extraction and transformation.
    """
    try:
        await create_necessary_dirs_where_needed()
        configure_logging()
        logging.info("Client Start")
        dates = get_latest_dates()
        tasks_raw = construct_tasks_list(dates)
        tasks = validate_tasks(tasks_raw)
        async with AsyncClient(params={"api_key": os.getenv("API_KEY")}) as http_client:
            await extract_data_from_apis_to_disk(http_client, tasks)
        await convert_all_json_files_to_jsonl(tasks)
        transform_input_files(tasks)
    except Exception as e:
        logging.exception("Something went wrong")


async def create_necessary_dirs_where_needed():
    for dir in (logs_dir, output_dir):
        if not await aiofiles.os.path.exists(dir):
            await aiofiles.os.makedirs(dir)


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


def validate_tasks(tasks_raw: List[dict]) -> List[Task]:
    """
    Validates a list of unvalidated ('raw') tasks and returns a list of validated tasks.
    """
    validated_tasks = []
    for task in tasks_raw:
        validated_tasks.append(Task(**task))
    logging.info("Task configuration(s) are valid")
    return validated_tasks


async def extract_data_from_apis_to_disk(http_client: AsyncClient, tasks: List[Task]) -> None:
    """
    Extracts data from multiple APIs asynchronously and saves it to disk using streaming.

    :param http_client: An instance of `AsyncClient` used for making HTTP requests.
    :param tasks: A list of `Task` objects representing the API endpoints to retrieve data from.
    """
    requests = []
    for task in tasks:
        requests.append(asyncio.ensure_future(send_request_and_stream_response_to_disk(http_client, task)))
    with catchtime() as duration:
        results = await asyncio.gather(*requests)
    logging.info({"message": f"Successfully streamed input file(s)", "duration": duration(), "results": results})


def get_chunked_df(task):
    chunksize = 100
    if task.input_data_format == "json":
        return pd.read_json(task.temp_file, chunksize=chunksize, lines=True)
    else:
        return pd.read_csv(task.temp_file, chunksize=chunksize)


@dask.delayed
def transform(task: Task) -> (str, str):
    with catchtime() as duration:
        write_header = True
        for chunk in get_chunked_df(task):
            chunk.columns = chunk.columns.str.strip()
            column_rename_mapping = {
                "Last Modified utc": "last_modified",
                "Variable": "variable",
                "Naive_Timestamp": "time_stamp",
            }
            chunk.rename(columns=column_rename_mapping, inplace=True)
            chunk['variable'] = chunk['variable'].astype(int)
            chunk['value'] = chunk['value'].astype(float)
            unit = 'ms' if task.input_data_format == 'json' else None
            chunk['time_stamp'] = pd.to_datetime(chunk['time_stamp'], utc=True, unit=unit)
            chunk['last_modified'] = pd.to_datetime(chunk['last_modified'], utc=True, unit=unit)
            write_header = load_to_file(chunk, write_header, task)
    return task.output_filepath, duration()


def load_to_file(chunk, write_header, task):
    pd.DataFrame.to_csv(chunk, task.output_filepath, index=False, mode='a', header=write_header)
    write_header = False
    return write_header


@tenacity.retry(wait=tenacity.wait_exponential(multiplier=0.02, min=0.01, max=0.5))
async def send_request_and_stream_response_to_disk(client: AsyncClient, task: Task) -> (str, str):
    """
    Sends a GET request to the specified URL and processes the response stream asynchronously.

    Uses retry behavior with an exponential backoff strategy.

    Parameters: - client (AsyncClient): The HTTP client used to send the request. - task (Task): An object containing
    information about the request task, including the input data URL and temporary file path.

    Raises:
    - HTTPError: If the response status code indicates an error.

    """
    with catchtime() as duration:
        async with aiofiles.open(task.temp_file, mode='wb') as tmp_file:
            async with client.stream('GET', str(task.input_data_url)) as response:
                response.raise_for_status()
                async for chunk in response.aiter_bytes(chunk_size=4096):
                    await tmp_file.write(chunk)
    return task.temp_file, duration()


@contextmanager
def catchtime() -> str:
    """Return in milliseconds, how long the context took to execute."""
    start = perf_counter()
    yield lambda: f"{(perf_counter() - start) * 1000:.0f}ms"


if __name__ == '__main__':
    with catchtime() as duration:
        asyncio.run(run_etl_pipeline())
    logging.info({"message": "Client Shutdown", "total_duration": duration()})
