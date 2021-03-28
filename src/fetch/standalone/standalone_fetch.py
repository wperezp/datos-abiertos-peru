import boto3
import requests
import hashlib
import os
import sys
import yaml
import math
from hurry.filesize import size


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


def fetch_all_datasets(f_catalog: dict):
    for key, item in f_catalog.items():
        asset_name = item['Name']
        asset_filename = item['Filename']
        asset_url = item['URI']
        print(f"{key} Asset name: {asset_name}")
        if asset_exists(asset_name):
            print(f"{key} Already uploaded")
            continue
        print(f"Downloading from {asset_url}")
        # response = requests.get(asset_url, stream=True)
        response = requests.get(asset_url)
        # full_content = bytes(0)
        total_length = response.headers.get('Content-Length')
        if total_length is not None:
            print(f'File size: {size(int(total_length))}')
            # dl = 0
            # prg = 0
            # for data in response.iter_content(chunk_size=None):
            #     dl += len(data)
            #     full_content += data
            #     avance = math.floor((dl*100)/int(total_length))
            #     if avance > prg:
            #         print(f"{avance} %")
            #         prg = avance
        else:
            print('Total file length not available in headers')
        full_content = response.content
        if is_newer_version(asset_name, full_content):
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(os.environ['S3_DATA_BUCKET'])
            bucket.put_object(Key=f'raw/{asset_filename}', Body=full_content)
        del full_content


def parse_catalog(filename: str):
    f_content = open(filename, 'r').read()
    return yaml.safe_load(f_content)


if __name__ == '__main__':
    catalog_filename = sys.argv[1]
    catalog = parse_catalog(catalog_filename)
    fetch_all_datasets(catalog)
