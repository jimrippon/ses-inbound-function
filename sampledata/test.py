import os
import logging
import boto3
import uuid
import hashlib
from email import policy
from email.parser import BytesParser

# Environment variables - these are the configurables 
SOURCE='s3'                                     # retrieve from s3
#SOURCE='file'                                  # retrieve from local file
OBJECT_NAME='sample.eml'                        # filename
SES_IN_BUCKET='cloudscale.uk.mail'              # Source S3 bucket
SES_IN_PREFIX='incoming/'                       # Source S3 prefix
PROCESSOR_BUCKET='csc-occupancy-processor'      # Target S3 bucket
PROCESSOR_PREFIX='pending/'                     # Target S3 prefix
PROCESSOR_OBJECT_NAME=str(uuid.uuid4())         # Generate a random uuid for the output object name

# Initialise the logger
logger = logger.getLogger(__name__)
logger.setLevel(logging.INFO)

# Some functions we are going to use
def UploadBytesToS3(bytes, bucket, object_name=None):
    """Upload the provided bytestring to AWS S3
    
    Keyword arguments:
    bytes - the binary bytestring to upload
    bucket - string containing the name of the S3 bucket
    object_name - string containing the name of the object to create

    Returns dictionary containing the following:
    status - status string "success" or "fail"
    object_name - the name of the object uploaded
    object_url - the direct url to the object uploaded
    """
    retval = {}

    if object_name is None:
        # Generate random UUID as object_name if none is supplied
        object_name = uuid.uuid4()
    
    try:
        s3_client = boto3.client('s3')
        object = s3.Object(bucket, object_name)
        response = object.put(ACL='private', StorageClass='ReducedRedundancy', body=bytes)
    except:
        retval['status'] = 'fail'
    finally:
        return retval

# Retrieve the source message
try:
    if SOURCE=='s3':
        logger.info('Attempting to retrieve object {} from S3 bucket {}'.format(SES_IN_PREFIX+OBJECT_NAME, SES_IN_BUCKET))
        with s3.get_object(Bucket=SES_IN_BUCKET, Key=SES_IN_PREFIX+OBJECT_NAME) as f:
            msg = BytesParser(policy=policy.strict().parse(f)
    elif SOURCE='file':
        logger.info('Attempting to retrieve local file {}'.format(OBJECT_NAME))
        with open(OBJECT_NAME, 'rb') as f:
            msg = BytesParser(policy=policy.strict).parse(f)
    else:
        raise Exception('SOURCE value not recognized, please check the supplied configuration')
except:
    print('Unable to retrieve source file, bailing out')
else:
    for att in msg.iter_attachments():
        fn = att.get_filename()

        if fn and att.get_content_type == 'text/csv':
            
            with open(fn, 'wb') as f:
                f.write(att.get_payload(decode=True))

            # We now need to write the attachment to S3
        else:
            # ignore this attachment
            logger.info('Attachment being ignored')
