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


def invoke_fargate(task_definition: str, cluster_name: str, asset_name: str, asset_filename: str,
                   asset_url: str):
    ecs_client = boto3.client('ecs')
    response = ecs_client.run_task(
        cluster=cluster_name,
        taskDefinition=task_definition,
        overrides={
            'containerOverrides': [
                {
                    'environment': [
                        {'name': 'ASSET_NAME', 'value': asset_name},
                        {'name': 'ASSET_FILENAME', 'value': asset_filename},
                        {'name': 'ASSET_URL', 'value': asset_url}
                    ]
                }
            ]
        }
    )
    print(response)


def fetch_dataset(asset_name: str, asset_filename: str, asset_url: str, upload_only_once=False, lambda_context=None,
                  invoke_fargate=False):
    print(f"Asset name: {asset_name}")
    # if upload_only_once:
    #     print(f"Checking if it's already uploaded")
    #     if asset_exists(asset_name):
    #         print(f"{key} Already uploaded")
    #         return
    print(f"Downloading from {asset_url}")
    response = requests.get(asset_url, stream=True)
    full_content = bytes(0)
    total_length = response.headers.get('Content-Length')
    if total_length is not None:
        total_length = int(total_length)
        print(f'File size: {round(float(total_length) / 1048576, 2)}M')
        dl = 0
        prg = 0
        chunk_size = 1048576
        start = time.perf_counter()
        count = 0
        for data in response.iter_content(chunk_size=chunk_size):
            dl += len(data)
            full_content += data
            done = float(dl) / total_length
            elapsed = time.perf_counter() - start
            speed = (float(dl)) / elapsed
            eta = float(total_length - dl) / speed
            count += 1
            if prg < int(done * 100):  # Imprimir cada 1% por lo menos
                prg = int(done * 100)
                print(f"{prg}% {round(speed / 1000000, 2)} Mbps ETA {int(eta)}s")
            if count == 10 and invoke_fargate:  # Evaluar si disparar fargate despues de los primeros 10MB de descarga
                remaining = int(lambda_context.get_remaining_time_in_millis() / 1000)
                if (eta + 30) > remaining:
                    print("ETA is longer than remaining time for this function. Transfer to Fargate")
                    return False
    else:
        print('Total file length not available in headers')
    full_content = response.content
    if is_newer_version(asset_name, full_content):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(os.environ['S3_DATA_BUCKET'])
        bucket.put_object(Key=f'raw/{asset_filename}', Body=full_content)
    return True


def parse_catalog(filename: str):
    f_content = open(filename, 'r').read()
    return yaml.safe_load(f_content)


def lambda_handler(event, context):
    asset_name = event['asset_name']
    asset_filename = event['asset_filename']
    asset_url = event['asset_url']
    upload_only_once = event.get('cron_expression') is None
    task_definition = os.environ['TASK_DEFINITION']
    cluster_name = os.environ['CLUSTER_NAME']
    function_finished = fetch_dataset(asset_name, asset_filename, asset_url, upload_only_once, context, True)
    if not function_finished:
        invoke_fargate(task_definition, cluster_name, asset_name, asset_filename, asset_url)


if __name__ == '__main__':
    if os.environ['EXEC_MODE'] == 'FARGATE':
        asset_name = os.environ['ASSET_NAME']
        asset_filename = os.environ['ASSET_FILENAME']
        asset_url = os.environ['ASSET_URL']
        upload_only_once = os.environ.get('CRON_EXPRESSION') is None
        fetch_dataset(asset_name, asset_filename, asset_url, upload_only_once)
    elif os.environ['EXEC_MODE'] == 'LOCAL':
        f_catalog = parse_catalog('catalog.yml')
        for key, item in f_catalog.items():
            asset_name = item['Name']
            asset_filename = item['Filename']
            asset_url = item['URI']
            upload_only_once = item.get('CronExpression') is None
            fetch_dataset(asset_name, asset_filename, asset_url, upload_only_once)
