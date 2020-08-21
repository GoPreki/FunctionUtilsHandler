import json
from decimal import Decimal

LIST_SEPARATOR = '@@'


def stringify_message(message):
    if isinstance(message, dict):
        return json.dumps(message, ensure_ascii=False)
    elif isinstance(message, list):
        return LIST_SEPARATOR.join(message)
    return str(message)


def parse_message(message):
    try:
        return json.loads(message)
    except json.decoder.JSONDecodeError:
        if LIST_SEPARATOR in message:
            return message.split(LIST_SEPARATOR)
        return message


class DecimalEncoder(json.JSONEncoder):

    def default(self, o):
        if isinstance(o, Decimal):
            return float(o) if o % 1 > 0 else int(o)
        return super(DecimalEncoder, self).default(o)


class Parser:

    @staticmethod
    def to_number(data):
        return json.loads(json.dumps(data, cls=DecimalEncoder))

    @staticmethod
    def to_decimal(data):
        return json.loads(json.dumps(data), parse_float=Decimal)
