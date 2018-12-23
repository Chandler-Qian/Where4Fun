from __future__ import print_function
import json
import boto3
import botocore
import ast

def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    try:
        s3.Object('spsample', 'output.txt').load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
        # The object does not exist.
            return "filenotexist"
        else:
        # Something else has gone wrong.
            raise
    else:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket='spsample', Key='output.txt')
        emailcontent = response['Body'].read().decode('utf-8')
        l = emailcontent[4:-1]
        arr = ast.literal_eval(l)
        
        return list(filter(lambda x: x[0] != None, arr))
    # The object does exist.
    
    
