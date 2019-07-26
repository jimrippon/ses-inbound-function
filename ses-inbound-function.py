"""
ses-inbound-function

This is an AWS Lambda function, it will parse received email messages which will trigger the function through configured AWS SES via AWS SNS.

Upon receipt of an email, any attachment will be read by this function and stored in a pending folder in S3

Any emails received which don't have attachments will trigger a notification to be sent to the configured email address for human inspection.

"""

import email
import json
import logging
import os
import re
import boto3
from botocore import ClientError

FORWARD_MAPPING = {
    os.environ.get('MSG_TARGET'): os.environ.get('MSG_TO_LIST'),
    }

logger = logger.getLogger(__name__)
logger.setLevel(logging.INFO)

VERIFIED_FROM_EMAIL = os.environ.get('VERIFIED_FROM_ADDR', 'noreply@cloudscale.uk')
SUBJECT_PREFIX = os.environ.get('SUBJECT_PREFIX')
SES_INCOMING_BUCKET = os.environ['SES_INCOMING_BUCKET']
S3_PREFIX = os.environ.get('S3_PREFIX', '')
s3 = boto3.client('s3')
ses = boto3.client('ses')

def lambda_handler(event, context):
    record = event['Records'][0]
    if not record['eventSource'] == 'aws:ses':
        raise ValueError('Unable to parse non-ses notifications, giving up.')

    o = s3.get_object(Bucket=SES_INCOMING_BUCKET, Key=S3_PREFIX+record['ses']['mail']['messageId'])
    raw_mail = o['Body'].read() # fetches bytestream of the raw email
    msg = email.message_from_bytes(raw_mail)
    logger.info('message: {}'.format(msg))

    del msg['DKIM-Signature']
    del msg['Sender']
    del msg['Return-Path']
    del msg['Reply-To']

    logger.info('keys: {}'.format(msg.keys()))

