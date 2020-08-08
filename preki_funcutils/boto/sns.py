import boto3
from .utils import stringify_message


def send_topic_message(topic_arn, message, **kwargs):
    sns = boto3.resource('sns')
    sns.publish(
        TargetArn=topic_arn,
        Message=stringify_message(message),
        **kwargs,
    )
