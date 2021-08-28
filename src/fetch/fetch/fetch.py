import boto3
import requests
import yaml
import hashlib
import os
import time


def is_newer_version(asset_name: str, obj_bytes: bytes) -> bool:
    dynamodb = boto3.resource('dynamodb')
    hashes_table = dynamodb.Table(os.environ['DDB_HASHES_TABLE'])
    obj_md5 = hashlib.md5(obj_bytes).hexdigest()
    response = hashes_table.get_item(
        Key={
            'asset_name': asset_name
        }
    )
    try:
        response_item = response['Item']
        current_md5 = response_item['md5_hash']
        is_new = obj_md5 != current_md5
    except KeyError as e:
        is_new = True
    finally:
        hashes_table.put_item(
            Item={
                'asset_name': asset_name,
                'md5_hash': obj_md5
            }
        )
    return is_new


def asset_exists(asset_name: str) -> bool:
    dynamodb = boto3.resource('dynamodb')
    hashes_table = dynamodb.Table(os.environ['DDB_HASHES_TABLE'])
    response = hashes_table.get_item(
        Key={
            'asset_name': asset_name
        }
    )
    try:
        response_item = response['Item']
        return response_item is not None
    except KeyError as e:
        return False


def fetch_dataset(asset_name: str, asset_filename: str, asset_url: str, upload_only_once=False):
    print(f"Asset name: {asset_name}")
    if upload_only_once:
        print(f"Checking if it's already uploaded")
        if asset_exists(asset_name):
            print(f"Already uploaded")
            return
    print(f"Downloading from {asset_url}")
    response = requests.get(asset_url)
    full_content = response.content
    if is_newer_version(asset_name, full_content):
        print("Downloaded file is new. Uploading to S3")
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(os.environ['S3_DATA_BUCKET'])
        bucket.put_object(Key=f'raw/{asset_filename}', Body=full_content)
    else:
        print("File already exists")


def parse_catalog(filename: str):
    f_content = open(filename, 'r').read()
    return yaml.safe_load(f_content)


if __name__ == '__main__':
    print(os.environ)
    if os.environ['EXEC_MODE'] == 'FARGATE':
        asset_name = os.environ['ASSET_NAME']
        asset_filename = os.environ['ASSET_FILENAME']
        asset_url = os.environ['ASSET_URL']
        cron_expression = os.environ['CRON_EXPRESSION']
        upload_only_once = cron_expression == ""
        fetch_dataset(asset_name, asset_filename, asset_url, upload_only_once)
    elif os.environ['EXEC_MODE'] == 'LOCAL':
        f_catalog = parse_catalog('catalog.yml')
        for key, item in f_catalog.items():
            asset_name = item['Name']
            asset_filename = item['Filename']
            asset_url = item['URI']
            upload_only_once = item.get('CronExpression') is None
            fetch_dataset(asset_name, asset_filename, asset_url, upload_only_once)
