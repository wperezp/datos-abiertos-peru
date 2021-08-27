import boto3
import yaml
import os
import sys
import json


def parse_catalog(filename: str):
    f_content = open(filename, 'r').read()
    return yaml.safe_load(f_content)


def invoke_for_all_assets(catalog: dict, stmxn_arn: str):
    sfn_client = boto3.client('stepfunctions')
    for key, item in catalog.items():
        asset_name = item['Name']
        print(f"Invoke {asset_name}")
        asset_filename = item['Filename']
        asset_url = item['URI']
        payload = {
            'asset_name': asset_name,
            'asset_filename': asset_filename,
            'asset_url': asset_url
        }
        if item.get('CronExpression') is not None:
            payload['cron_expression'] = item['CronExpression']
        sfn_client.start_execution(
            stateMachineArn=stmxn_arn,
            input=json.dumps(payload)
        )


def lambda_handler(event, context):
    catalog_to_invoke = parse_catalog('catalog.yml')
    stmxn_arn = os.environ['SFN_STMXN_ARN']
    invoke_for_all_assets(catalog_to_invoke, stmxn_arn)


if __name__ == '__main__':
    catalog_to_invoke = parse_catalog('catalog.yml')
    function_name = sys.argv[1]
    invoke_for_all_assets(catalog_to_invoke, function_name)