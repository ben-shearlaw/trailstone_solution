import asyncio
import logging
import os
from typing import List

import aiofiles.os
from httpx import AsyncClient, HTTPStatusError
import ijson
import simplejson as json
import tenacity

from helpers import catchtime, Task
from settings import STREAM_CHUNK_SIZE


async def convert_json_file_to_jsonl(task: Task):
    async with aiofiles.open(task.temp_file, 'r') as json_file:
        async with aiofiles.open(f"{task.temp_file}_reformatted", mode='w') as jsonl_file:
            async for record in ijson.items_async(json_file, "item"):
                await jsonl_file.write(json.dumps(record) + "\n")
    await aiofiles.os.rename(f"{task.temp_file}_reformatted", task.temp_file)


async def convert_json_files_to_jsonl(tasks: List[Task]):
    """This is done so that the data can be chunked by pandas."""
    with catchtime() as duration:
        conversion_jobs = []
        for task in tasks:
            if task.input_data_format == "json":
                conversion_jobs.append(asyncio.ensure_future(convert_json_file_to_jsonl(task)))
        await asyncio.gather(*conversion_jobs, return_exceptions=True)
    logging.info({"message": "converted JSON to JSONL", "duration": duration()})


async def extract_data_from_apis_to_disk(tasks: List[Task]):
    """
    Extracts data from multiple APIs asynchronously and saves it to disk using streaming.

    :param tasks: A list of `Task` objects representing the API endpoints to retrieve data from.
    """
    jobs = []
    async with AsyncClient(params={"api_key": os.getenv("API_KEY")}) as http_client:
        for task in tasks:
            jobs.append(asyncio.ensure_future(send_request_and_stream_response_to_disk(http_client, task)))
        with catchtime() as duration:
            results = await asyncio.gather(*jobs, return_exceptions=True)
    logging.info({"message": f"Successfully streamed input file(s)", "duration": duration(), "results": results})


@tenacity.retry(
    retry=tenacity.retry_if_exception_type(HTTPStatusError),
    wait=tenacity.wait_exponential(
        multiplier=0.01,
        min=0.01,
        max=0.5),
)
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
                async for chunk in response.aiter_bytes(chunk_size=STREAM_CHUNK_SIZE):
                    await tmp_file.write(chunk)
    return task.temp_file, duration()
