import boto3
import os


def invoke_fargate(task_definition: str, container_name: str, cluster_name: str, subnet_id: str, asset_name: str,
                   asset_filename: str, asset_url: str):
    ecs_client = boto3.client('ecs')
    response = ecs_client.run_task(
        cluster=cluster_name,
        taskDefinition=task_definition,
        overrides={
            'containerOverrides': [
                {
                    'name': container_name,
                    'environment': [
                        {'name': 'ASSET_NAME', 'value': asset_name},
                        {'name': 'ASSET_FILENAME', 'value': asset_filename},
                        {'name': 'ASSET_URL', 'value': asset_url},
                        {'name': 'EXEC_MODE', 'value': 'FARGATE'}
                    ]
                }
            ]
        },
        count=1,
        launchType='FARGATE',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': [subnet_id]
            }
        }
    )
    print(response)


def lambda_handler(event, context):
    asset_name = event['asset_name']
    asset_filename = event['asset_filename']
    asset_url = event['asset_url']
    task_definition = os.environ['TASK_DEFINITION']
    cluster_name = os.environ['CLUSTER_NAME']
    container_name = os.environ['CONTAINER_NAME']
    subnet_id = os.environ['SUBNET_ID']
    invoke_fargate(task_definition, container_name, cluster_name, subnet_id, asset_name, asset_filename, asset_url)
