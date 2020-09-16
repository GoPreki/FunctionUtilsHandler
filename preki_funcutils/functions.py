import json
import logging
from enum import Enum
from functools import wraps
from . import status, exceptions
from .utils import parse_message


def _make_response(body, status_code=status.HTTP_200_OK):
    return {'statusCode': status_code, 'headers': {'Content-Type': 'application/json'}, 'body': json.dumps(body)}


def _make_error(type, message, error_code, status_code):
    error = {'type': type, 'message': message, 'error_code': error_code}
    return _make_response(body={'error': error}, status_code=status_code)


def _determine_protocol(event):
    if 'Records' in event:
        records = event['Records']
        if len(records) and records[0].get('EventSource', None) == 'aws:sns':
            return Protocol.SNS
        return Protocol.SQS
    else:
        return Protocol.HTTP


def lambda_response(func):

    @wraps(func)
    def wrapper(event, context, *args, **kwargs):
        protocol = _determine_protocol(event)
        try:
            if protocol == Protocol.HTTP:
                event['body'] = parse_message(event.get('body', None) or '{}')
                event['queryStringParameters'] = event.get('queryStringParameters', None) or {}
                event['pathParameters'] = event.get('pathParameters', None) or {}
            elif protocol == Protocol.SQS:
                for i, r in enumerate(event['Records']):
                    body = event['Records'][i]['body']
                    event['Records'][i]['body'] = parse_message(body)
                    if isinstance(body, dict) and body.get('Type', '') == 'Notification' and 'Message' in body:
                        event['Records'][i]['body']['Message'] = parse_message(body['Message'])

            response = func(event, context, *args, **kwargs)
            return _make_response(body=response)
        except exceptions.PrekiException as e:
            logging.warning(e)
            return _make_error(type=type(e).__name__,
                               message=e.message,
                               error_code=e.error_code,
                               status_code=e.status_code)
        except Exception as e:
            logging.exception(e)
            if protocol == Protocol.HTTP:
                return _make_error(type=type(e).__name__,
                                   message=str(e),
                                   error_code=None,
                                   status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
            elif protocol == Protocol.SQS:
                raise e

    return wrapper


class Protocol(Enum):
    HTTP = 'HTTP'
    SQS = 'SQS'
    SNS = 'SNS'
