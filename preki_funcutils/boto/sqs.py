import boto3
from ..utils import chunks, stringify_message


def enqueue_message(queue_name, message, **kwargs):
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    return queue.send_message(
        MessageBody=stringify_message(message),
        **kwargs,
    )


def enqueue_messages_batch(queue_name, messages, **kwargs):
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=queue_name)

    response = {
        'Successful': [],
        'Failed': [],
    }

    for messages_chunk in chunks(messages, 10):
        chunk_response = queue.send_messages(Entries=[{
            'Id': f'{i}',
            'MessageBody': stringify_message(m),
            **kwargs,
        } for i, m in enumerate(messages_chunk)])

        response['Successful'].extend(chunk_response.get('Successful', []))
        response['Failed'].extend(chunk_response.get('Failed', []))

    return response
