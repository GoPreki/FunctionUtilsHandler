import json
import re
from functools import wraps
from preki_funcutils.logger import LogLevel, log
from .utils import parse_message
from .internals import LambdaContext, LambdaEvent, Protocol, find_entity
from . import status, exceptions


def _make_response(origin, stage, body, status_code=status.HTTP_200_OK):

    allowed_hosts = [
        'gopreki.com',
        'preki.com',
        'preki.co',
    ]

    is_allowed = bool([
        host for host in allowed_hosts if re.match(
            rf'^((http|https|capacitor):\/\/)?([a-zA-Z0-9]*\.)*{re.escape(host)}((\.[a-zA-Z]+)+)?(\/.*)?$', origin)
    ])

    if not is_allowed:
        is_localhost = re.match(r'((http|https|capacitor):\/\/)?localhost((:|\/).+)?$', origin)
        is_allowed = stage == 'dev' and is_localhost

    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': origin if is_allowed else 'http://preki.com',
            'Access-Control-Allow-Credentials': True,
        },
        'body': json.dumps(body)
    }


def _make_error(origin, stage, type, message, error_code, status_code, data):
    error = {'type': type, 'message': message, 'error_code': error_code}
    return _make_response(origin=origin, stage=stage, body={'error': error, 'data': data}, status_code=status_code)


def _determine_protocol(event):
    if 'Records' in event:
        records = event['Records']
        if len(records) and records[0].get('EventSource', None) == 'aws:sns':
            return Protocol.SNS
        return Protocol.SQS
    else:
        return Protocol.HTTP


def _set_lambda_context(context):
    LambdaContext.set({
        'function_name': context.function_name,
        'function_version': context.function_version,
    })


def _set_lambda_event(protocol: Protocol, event=None):
    extra = {}
    if protocol == Protocol.HTTP:
        try:
            path = event['path']
            entity, id = find_entity(path=path)
            extra = {'entity_type': entity}
            if id:
                extra['entity_id'] = id
        except Exception:
            pass

    LambdaEvent.set({
        'protocol': protocol.value,
        **extra,
    })


def lambda_response(func):

    @wraps(func)
    def wrapper(event, context, *args, **kwargs):
        _set_lambda_context(context)
        protocol = _determine_protocol(event)
        headers = event.get('headers', {})
        origin = headers.get('origin', headers.get('Origin', ''))
        stage = event.get('requestContext', {}).get('stage', 'dev')

        try:
            if protocol == Protocol.HTTP:
                event['body'] = parse_message(event.get('body', None) or '{}')
                event['queryStringParameters'] = event.get('queryStringParameters', None) or {}
                event['pathParameters'] = event.get('pathParameters', None) or {}
            elif protocol == Protocol.SQS:
                for i, r in enumerate(event['Records']):
                    body = parse_message(event['Records'][i]['body'])
                    event['Records'][i]['body'] = body
                    if isinstance(body, dict) and body.get('Type', '') == 'Notification' and 'Message' in body:
                        event['Records'][i]['body']['Message'] = parse_message(body['Message'])
            elif protocol == Protocol.SNS:
                for i, r in enumerate(event['Records']):
                    event['Records'][i]['Sns']['Message'] = parse_message(event['Records'][i]['Sns']['Message'])

            _set_lambda_event(protocol=protocol, event=event)
            response = func(event, context, *args, **kwargs)
            return _make_response(origin=origin, stage=stage, body=response)
        except exceptions.PrekiException as e:
            log(level=LogLevel.WARNING, message=e)
            return _make_error(origin=origin,
                               stage=stage,
                               type=type(e).__name__,
                               message=e.message,
                               error_code=e.error_code,
                               status_code=e.status_code,
                               data=e.data)
        except Exception as e:
            log(level=LogLevel.CRITICAL, message=e)
            if protocol == Protocol.HTTP:
                return _make_error(origin=origin,
                                   stage=stage,
                                   type=type(e).__name__,
                                   message=str(e),
                                   error_code=None,
                                   status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                   data=None)
            elif protocol == Protocol.SQS:
                raise e

    return wrapper
