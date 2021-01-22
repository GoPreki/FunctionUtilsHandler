import boto3
from enum import Enum
from typing import Union
from ..utils import Parser


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


def scan(table_name, ComparisonOperator, ExclusiveStartKey=None, ExpressionAttributeValues=None, **kwargs):
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

    response = table.scan(
        ComparisonOperator=ComparisonOperator, **ExclusiveStartKey, **ExpressionAttributeValues, **kwargs)

    return Parser.to_number(response.get('Items', None)), {
        'Count': response.get('Count', None),
        'ScannedCount': response.get('ScannedCount', None),
        'LastEvaluatedKey': response.get('LastEvaluatedKey', None),
    }
