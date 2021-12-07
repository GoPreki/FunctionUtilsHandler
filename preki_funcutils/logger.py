import logging
from enum import Enum
from .internals import LambdaContext, LambdaEvent


class LogLevel(Enum):
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITITAL = logging.CRITICAL


def log(level: LogLevel = LogLevel.INFO, message='', args={}):
    lambda_context = LambdaContext.get()
    lambda_event = LambdaEvent.get()

    exec_info = True if level in [LogLevel.ERROR, LogLevel.CRITITAL] else None

    logging.log(
        level.value,
        msg={
            'message': message,
            **args,
            **lambda_context,
            **lambda_event,
        },
        exc_info=exec_info,
    )
