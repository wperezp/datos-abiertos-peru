import boto3
import cleaning


def lambda_handler(event, context):
    print(event)
    print(context)
