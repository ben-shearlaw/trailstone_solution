import asyncio
import logging
import logging.config

from dotenv import load_dotenv

from extract import convert_json_files_to_jsonl, extract_data_from_apis_to_disk
from helpers import create_necessary_dirs_if_needed, catchtime, assemble_list_of_tasks, cleanup_temp_files
from settings import configure_logging
from transform import transform_input_files

load_dotenv()


async def run_etl_client() -> None:
    """Runs the ETL (Extract, Transform, Load) client."""
    try:
        await create_necessary_dirs_if_needed()
        configure_logging()
        logging.info("Client Start")
        tasks = assemble_list_of_tasks()
        await extract_data_from_apis_to_disk(tasks)
        await convert_json_files_to_jsonl(tasks)
        transform_input_files(tasks)
        await cleanup_temp_files(tasks)
    except Exception:
        logging.exception("An error has occurred. See attached stacktrace.")


if __name__ == '__main__':
    with catchtime() as duration:
        asyncio.run(run_etl_client())
    logging.info({"message": "Client Shutdown", "total_duration": duration()})
