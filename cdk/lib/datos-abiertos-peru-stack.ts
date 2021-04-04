import * as cdk from '@aws-cdk/core';
import * as s3 from '@aws-cdk/aws-s3';
import * as dynamodb from '@aws-cdk/aws-dynamodb'
import * as lambda from '@aws-cdk/aws-lambda'
import * as ecs from '@aws-cdk/aws-ecs'
import * as iam from '@aws-cdk/aws-iam'
import { Duration, FeatureFlags } from '@aws-cdk/core';
import { DAPScheduledFetchEvents } from './daily-fetch-events'
import { DAPFetchContainer } from './single-fetch-container';

export class DatosAbiertosPeruStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);
    
    const dataBucket = new s3.Bucket(this, 'source-data', {
      versioned: false
    })

    const hashesTable = new dynamodb.Table(this, 'md5_hashes', {
      partitionKey: {
        name: 'asset_name',
        type: dynamodb.AttributeType.STRING
      },
      readCapacity: 1,
      writeCapacity: 1,
    })

    const ecsCluster = new ecs.Cluster(this, 'ecsCluster', {containerInsights: true})

    const fetchFargate = new DAPFetchContainer(this, 'singleFetch', {
      S3_DATA_BUCKET: dataBucket.bucketName,
      DDB_HASHES_TABLE: hashesTable.tableName,
      EXEC_MODE: "FARGATE"
    })

    hashesTable.grantReadWriteData(fetchFargate.taskDefinition.taskRole)
    dataBucket.grantWrite(fetchFargate.taskDefinition.taskRole)

    const fetchFn = new lambda.Function(this, 'fnScheduledFetch', {
      handler: 'fetch.lambda_handler',
      runtime: lambda.Runtime.PYTHON_3_8,
      code: lambda.Code.fromAsset('../src/fetch/'),
      memorySize: 1500,
      timeout: Duration.minutes(15),
      environment: {
        "DDB_HASHES_TABLE": hashesTable.tableName,
        "S3_DATA_BUCKET": dataBucket.bucketName,
        "TASK_DEFINITION": fetchFargate.taskDefinition.taskDefinitionArn,
        "CLUSTER_NAME": ecsCluster.clusterName,
        "CONTAINER_NAME": fetchFargate.container.containerName
      }
    })

    hashesTable.grantReadWriteData(fetchFn)
    dataBucket.grantWrite(fetchFn)

    const grantEcsRunTask = new iam.PolicyStatement();
    grantEcsRunTask.addActions('ecs:RunTask')
    grantEcsRunTask.addResources(fetchFargate.taskDefinition.taskDefinitionArn)

    const grantPassRole = new iam.PolicyStatement();
    grantPassRole.addActions('iam:PassRole')
    grantPassRole.addResources(fetchFargate.taskDefinition.executionRole!.roleArn)

    fetchFn.addToRolePolicy(grantEcsRunTask)
    fetchFn.addToRolePolicy(grantPassRole)

    const requestsLayerArn = `arn:aws:lambda:${process.env.AWS_REGION}:770693421928:layer:Klayers-python38-requests-html:37`
    const requestsLayer = lambda.LayerVersion.fromLayerVersionArn(this, 'fnLayerRequests', requestsLayerArn)
    const yamlLayerArn = `arn:aws:lambda:${process.env.AWS_REGION}:770693421928:layer:Klayers-python38-PyYAML:4`
    const yamlLayer = lambda.LayerVersion.fromLayerVersionArn(this, 'fnLayerYAML', yamlLayerArn)
    
    fetchFn.addLayers(requestsLayer, yamlLayer)

    const invokeFetchFn = new lambda.Function(this, 'fnInvokeFetch', {
      handler: 'invoke.handler',
      runtime: lambda.Runtime.PYTHON_3_8,
      code: lambda.Code.fromAsset('../src/invoke'),
      environment: {
        "FETCH_FUNCTION_NAME": fetchFn.functionName
      },
      memorySize: 200,
      timeout: Duration.minutes(1)
    })
    
    invokeFetchFn.addLayers(yamlLayer)

    fetchFn.grantInvoke(invokeFetchFn)

    new DAPScheduledFetchEvents(this, 'scheduledFetch_Events', fetchFn)

  }
}
