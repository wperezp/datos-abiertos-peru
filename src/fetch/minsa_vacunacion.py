import boto3
import requests
import hashlib


def is_newer_version(asset_name: str, obj_bytes: bytes) -> bool:
    dynamodb = boto3.resource('dynamodb')
    hashes_table = dynamodb.Table('dap_md5_hashes')
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


def get_dataset(event, context):
    asset_url = 'https://cloud.minsa.gob.pe/s/ZgXoXqK2KLjRLxD/download'
    asset_name = 'minsa_vacunacion.csv'
    # asset_name = event['asset_name']
    # asset_url = event['asset_url']
    response = requests.get(asset_url)
    if is_newer_version(asset_name, response.content):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('dap-data-bucket')
        bucket.put_object(Key=f'raw/{asset_name}', Body=response.content)


get_dataset()
