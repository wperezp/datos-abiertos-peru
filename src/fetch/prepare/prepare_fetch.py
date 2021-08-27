import boto3
import requests


def lambda_handler(event, context):
    asset_name = event['asset_name']
    asset_filename = event['asset_filename']
    asset_url = event['asset_url']
    response = requests.get(asset_url, stream=True)
    total_length = response.headers.get('Content-Length')
    if total_length is not None:
        size_in_mb = round(float(total_length) / 1048576, 2)
        print(f"Asset file size: {size_in_mb} M")
    else:
        print(f"Can't determine file size. Assuming 8192 M")
        size_in_mb = 8192 # assume a default 8GB tops in case no content-length header is available
    return {
        "asset_name": asset_name,
        "asset_filename": asset_filename,
        "asset_url": asset_url,
        "size_in_mb": size_in_mb
    }
