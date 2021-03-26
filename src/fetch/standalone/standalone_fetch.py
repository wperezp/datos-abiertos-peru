import boto3
import requests
import hashlib
import os
import sys
import yaml
import math


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


def fetch_all_datasets(f_catalog: dict):
    for key, item in f_catalog.items():
        asset_filename = item['Filename']
        asset_url = item['URI']
        print(f"Downloading {key} from {asset_url}")
        response = requests.get(asset_url, stream=True)
        full_content = bytes(0)
        total_length = response.headers.get('Content-Length')
        if total_length is not None:
            dl = 0
            for data in response.iter_content(chunk_size=4096):
                dl += len(data)
                full_content += data
                sys.stdout.write(f"\r{math.floor((dl*100)/int(total_length))} %")
        else:
            full_content = response.content
        if is_newer_version(item['Name'], full_content):
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(os.environ['S3_DATA_BUCKET'])
            bucket.put_object(Key=f'raw/{asset_filename}', Body=full_content)


def parse_catalog(filename: str):
    f_content = open(filename, 'r').read()
    return yaml.safe_load(f_content)


if __name__ == '__main__':
    catalog_filename = sys.argv[1]
    catalog = parse_catalog(catalog_filename)
    fetch_all_datasets(catalog)
