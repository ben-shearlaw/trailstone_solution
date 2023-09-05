import contextvars
import logging
import os
import tempfile
import uuid
import logging.config

OUTPUT_DIR = "output"
LOGS_DIR = "logs"
TEMP_DIR = tempfile.gettempdir()
RUN_ID = contextvars.ContextVar("run_id", default=str(uuid.uuid4())[:8]).get()
DF_CHUNK_SIZE = 100
STREAM_CHUNK_SIZE = 4096


class IDFilter(logging.Filter):
    def filter(self, record):
        record.run_id = RUN_ID
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
                'filename': os.path.join(LOGS_DIR, f'{RUN_ID}.jsonl'),
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
