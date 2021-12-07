import logging
import json
from enum import Enum
from .internals import LambdaContext, LambdaEvent, LogLevelContext

PREKI_LOGGER_ID_KEY = 'logger_id'
PREKI_LOGGER_ID = 'preki-logger'
PREKI_LOGGER_ID = 'preki-logger'

logger = logging.getLogger(PREKI_LOGGER_ID)
if logger.handlers:
    logger.removeHandler(logger.handlers[0])
logger_handler = logging.StreamHandler()
logger_handler.setFormatter(logging.Formatter('%(message)s'))
logger.addHandler(logger_handler)


class LogLevel(Enum):
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


def parse(data):
    try:
        return json.dumps(data)
    except Exception:
        return data


def log(level: LogLevel = LogLevel.INFO, message='', args={}):
    lambda_context = LambdaContext.get()
    lambda_event = LambdaEvent.get()

    exec_info = True if level in [LogLevel.ERROR, LogLevel.CRITICAL] else None
    logger.setLevel(LogLevelContext.get())
    logger.log(
        level=level.value,
        msg=parse({
            'message': message,
            **args,
            **lambda_context,
            **lambda_event,
            'level': level.name,
            PREKI_LOGGER_ID_KEY: PREKI_LOGGER_ID,
        }),
        exc_info=exec_info,
    )
