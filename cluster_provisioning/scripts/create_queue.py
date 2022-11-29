import boto3
import configparser
import os
import logging
import json
from botocore.exceptions import ClientError

CONFIG_FILER_PATH = r'maap-hec-config.py'

config = configparser.ConfigParser()
config.read(CONFIG_FILER_PATH)
profile = config["AWS_SQS_QUEUE"]["profile"]
ades_base = config["AWS_SQS_QUEUE"]["ades_base"]
DELAY_SECONDS = config["AWS_SQS_QUEUE"].get("delay_seconds", '0')
VISIBLITY_TIMEOUT = config["AWS_SQS_QUEUE"].get("visibility_timeout",'60')

session = boto3.session.Session(profile_name=profile)
sqs_client = session.resource('sqs')

# logger config
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(levelname)s: %(message)s')


def create_queue(queue_name, delay_seconds=DELAY_SECONDS, visiblity_timeout=VISIBLITY_TIMEOUT):
    """
    Create a standard SQS queue
    """
    try:
        response = sqs_client.create_queue(QueueName=queue_name,
                                             Attributes={
                                                 'DelaySeconds': delay_seconds,
                                                 'VisibilityTimeout': visiblity_timeout
                                             })
    except ClientError:
        logger.exception(f'Could not create SQS queue - {queue_name}.')
        raise
    else:
        logger.info(
        f'Standard Queue {queue_name} created. Queue URL - {response.url}')


def main():
    create_queue("{}-wpst-request".format(ades_base))
    create_queue("{}-wpst-response".format(ades_base))
    create_queue("{}-wpst-job".format(ades_base))
    create_queue("{}-wpst-metrics".format(ades_base))

if __name__ == '__main__':
    main()
