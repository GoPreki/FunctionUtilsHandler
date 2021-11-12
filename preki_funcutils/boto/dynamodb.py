import boto3
from enum import Enum
from typing import Union
from ..utils import Parser, chunks


class CommonReturnValue(Enum):
    NONE = 'NONE'


class DeleteReturnValue(CommonReturnValue, Enum):
    ALL_OLD = 'ALL_OLD'


class PutReturnValue(DeleteReturnValue, Enum):
    pass


class UpdateReturnValue(DeleteReturnValue, Enum):
    ALL_NEW = 'ALL_NEW'
    UPDATED_OLD = 'UPDATED_OLD'
    UPDATED_NEW = 'UPDATED_NEW'


DeleteReturnValueType = Union[DeleteReturnValue, CommonReturnValue]
PutReturnValueType = Union[PutReturnValue, DeleteReturnValue, CommonReturnValue]
UpdateReturnValueType = Union[UpdateReturnValue, DeleteReturnValue, CommonReturnValue]


def get_item(table_name, Key, **kwargs):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    return Parser.to_number(table.get_item(Key=Key, **kwargs).get('Item', None))


def batch_get_item(table_name, Keys, **kwargs):
    if not table_name:
        raise Exception('Table name cannot be empty')
    dynamodb = boto3.resource('dynamodb')

    items = []
    for c in chunks(Keys, 100):
        items += _batch_get_item(dynamodb=dynamodb,
                                 RequestItems={
                                     table_name: {
                                         'Keys': c,
                                         'ConsistentRead': False
                                     }
                                 },
                                 **kwargs).get(table_name, [])

    return Parser.to_number(items)


def _batch_get_item(dynamodb, RequestItems, **kwargs):
    response = dynamodb.batch_get_item(
        RequestItems=RequestItems,
        **kwargs,
    )
    responses, unprocessed_keys = response.get('Responses', {}), response.get('UnprocessedKeys', {})

    if unprocessed_keys:
        unprocessed_responses = _batch_get_item(dynamodb=dynamodb, RequestItems=unprocessed_keys, **kwargs)
        for table, values in (unprocessed_responses or {}).items():
            if table in responses:
                responses[table] += values
                continue
            responses[table] = values

    return responses


def batch_write_item(table_name, PutItems=[], DeleteKeys=[], **kwargs):
    if not table_name:
        raise Exception('Table name cannot be empty')
    if not PutItems and not DeleteKeys:
        raise Exception('Requests cannot be empty')

    dynamodb = boto3.resource('dynamodb')

    requests = [
        *[{
            'PutRequest': {
                'Item': item
            }
        } for item in PutItems],
        *[{
            'DeleteRequest': {
                'Key': key
            }
        } for key in DeleteKeys],
    ]

    for c in chunks(requests, 25):
        _batch_write_item(dynamodb=dynamodb, RequestItems={table_name: c}, **kwargs)


def _batch_write_item(dynamodb, RequestItems, **kwargs):
    response = dynamodb.batch_write_item(
        RequestItems=RequestItems,
        **kwargs,
    )
    unprocessed_keys = response.get('UnprocessedKeys', {})

    if unprocessed_keys:
        _batch_write_item(dynamodb=dynamodb, RequestItems=unprocessed_keys, **kwargs)


def put_item(table_name, Item, ReturnValues: PutReturnValueType = PutReturnValue.NONE, **kwargs):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    Item = Parser.to_decimal(Item)

    response = table.put_item(Item=Item, ReturnValues=ReturnValues.value, **kwargs)

    if ReturnValues == DeleteReturnValue.ALL_OLD:
        return Parser.to_number(response.get('Attributes', None))


def update_item(table_name,
                Key,
                UpdateExpression,
                ExpressionAttributeValues=None,
                ReturnValues: UpdateReturnValueType = UpdateReturnValue.NONE,
                **kwargs):
    if ExpressionAttributeValues:
        ExpressionAttributeValues = {'ExpressionAttributeValues': Parser.to_decimal(ExpressionAttributeValues)}
    else:
        ExpressionAttributeValues = {}

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    response = Parser.to_number(
        table.update_item(
            Key=Key,
            UpdateExpression=UpdateExpression,
            ReturnValues=ReturnValues.value,
            **ExpressionAttributeValues,
            **kwargs,
        ))

    if ReturnValues != UpdateReturnValue.NONE:
        return Parser.to_number(response.get('Attributes', None))


def delete_item(table_name, Key, ReturnValues: DeleteReturnValueType = DeleteReturnValue.NONE, **kwargs):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    response = table.delete_item(Key=Key, ReturnValues=ReturnValues.value, **kwargs)

    if ReturnValues == DeleteReturnValue.ALL_OLD:
        return Parser.to_number(response.get('Attributes', None))


def query(table_name, KeyConditionExpression, **kwargs):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    response = table.query(KeyConditionExpression=KeyConditionExpression, **kwargs)

    return Parser.to_number(response.get('Items', None)), {
        'Count': response.get('Count', None),
        'ScannedCount': response.get('ScannedCount', None),
        'LastEvaluatedKey': response.get('LastEvaluatedKey', None),
    }


def query_all(table_name, KeyConditionExpression, **kwargs):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    return _query_all(table=table, KeyConditionExpression=KeyConditionExpression, **kwargs)


def _query_all(table, KeyConditionExpression, **kwargs):
    response = table.query(KeyConditionExpression=KeyConditionExpression, **kwargs)

    items, last_key = Parser.to_number(response.get('Items', None)), response.get('LastEvaluatedKey', None)
    if items and last_key:
        items += _query_all(
            table=table,
            KeyConditionExpression=KeyConditionExpression,
            **kwargs,
            ExclusiveStartKey=last_key,
        )

    return items


def scan(table_name, ExclusiveStartKey=None, ExpressionAttributeValues=None, **kwargs):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    if ExclusiveStartKey:
        ExclusiveStartKey = {'ExclusiveStartKey': Parser.to_decimal(ExclusiveStartKey)}
    else:
        ExclusiveStartKey = {}

    if ExpressionAttributeValues:
        ExpressionAttributeValues = {'ExpressionAttributeValues': Parser.to_decimal(ExpressionAttributeValues)}
    else:
        ExpressionAttributeValues = {}

    response = table.scan(**ExclusiveStartKey, **ExpressionAttributeValues, **kwargs)

    return Parser.to_number(response.get('Items', None)), {
        'Count': response.get('Count', None),
        'ScannedCount': response.get('ScannedCount', None),
        'LastEvaluatedKey': response.get('LastEvaluatedKey', None),
    }
