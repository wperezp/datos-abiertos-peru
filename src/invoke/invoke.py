import boto3
import yaml
import os
import sys
import json


def parse_catalog(filename: str):
    f_content = open(filename, 'r').read()
    return yaml.safe_load(f_content)


def invoke_for_all_assets(catalog: dict, function_name: str):
    lambda_client = boto3.client('lambda')
    for key, item in catalog.items():
        print(f"Invoke {item['Name']}")
        asset_filename = item['Filename']
        asset_url = item['URI']
        payload = {
            'asset_filename': asset_filename,
            'asset_url': asset_url
        }
        lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',
            Payload=json.dumps(payload).encode('utf-8')
        )


def lambda_handler(event, context):
    catalog_to_invoke = parse_catalog('catalog.yml')
    function_name = os.environ['FETCH_FUNCTION_NAME']
    invoke_for_all_assets(catalog_to_invoke, function_name)


if __name__ == '__main__':
    catalog_to_invoke = parse_catalog('catalog.yml')
    function_name = sys.argv[1]
    invoke_for_all_assets(catalog_to_invoke, function_name)