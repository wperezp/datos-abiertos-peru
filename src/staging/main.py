import boto3
import os
import cleaning
import importlib


def data_staging(asset_name, data):
    asset_module = importlib.import_module(f"cleaning.{asset_name}")
    asset_clean_func = getattr(asset_module, 'clean')
    cleaned_data = asset_clean_func(data['Body'].read())
    s3_key = "s3://{0}/staging/{1}.csv".format(os.environ['S3_SOURCE_BUCKET'], asset_name)
    cleaned_data.to_csv(s3_key, sep=';', index=False)


def lambda_handler(event, context):
    # check if coming from container or from lambda
    s3 = boto3.client('s3')
    if event.get('asset_name') is not None:
        asset_name = event['asset_name']
        asset_filename = event['asset_filename']
    else:
        container_env = event['Overrides']['ContainerOverrides'][0]['Environment']
        asset_dict = {}
        for item in container_env:
            asset_dict[item['Name']] = item['Value']
        asset_name = asset_dict['ASSET_NAME']
        asset_filename = asset_dict['ASSET_FILENAME']
    asset_obj = s3.get_object(Bucket=os.environ['S3_SOURCE_BUCKET'], Key=f'raw/{asset_filename}')
    data_staging(asset_name, asset_obj)


