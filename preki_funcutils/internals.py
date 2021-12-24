from enum import Enum
from contextvars import ContextVar
import logging

LambdaContext = ContextVar('LambdaContext', default={})
LambdaEvent = ContextVar('LambdaEvent', default={})
LogLevelContext = ContextVar('LambdaEvent', default=logging.INFO)


class Protocol(Enum):
    HTTP = 'HTTP'
    SQS = 'SQS'
    SNS = 'SNS'
    INVOKE = 'INVOKE'


def find_entity(path: str):
    id = None

    paths = path[1:].split('/', 2)
    if paths[1].isnumeric():
        id = paths[1]

    return paths[0], id
