from enum import Enum
from contextvars import ContextVar

LambdaContext = ContextVar('LambdaContext', default={})
LambdaEvent = ContextVar('LambdaEvent', default={})


class Protocol(Enum):
    HTTP = 'HTTP'
    SQS = 'SQS'
    SNS = 'SNS'


def find_entity(path: str):
    id = None

    paths = path[1:].split('/', 2)
    if paths[1].isnumeric():
        id = paths[1]

    return paths[0], id
