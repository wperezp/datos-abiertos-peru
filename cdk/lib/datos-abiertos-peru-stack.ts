import * as cdk from '@aws-cdk/core';
import * as s3 from '@aws-cdk/aws-s3';
import * as dynamodb from '@aws-cdk/aws-dynamodb'
import * as lambda from '@aws-cdk/aws-lambda'
import * as ecs from '@aws-cdk/aws-ecs'
import * as iam from '@aws-cdk/aws-iam'
import * as ec2 from '@aws-cdk/aws-ec2'
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

    const vpc = new ec2.Vpc(this, 'VPC', {
      maxAzs: 2,
      subnetConfiguration: [
        {
          name: 'default',
          subnetType: ec2.SubnetType.PUBLIC
        }
      ]
    })

    const ecsCluster = new ecs.Cluster(this, 'ecsCluster', {
      containerInsights: true,
      vpc: vpc
    })

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
        "S3_DATA_BUCKET": dataBucket.bucketName
      },
      retryAttempts: 1
    })

    hashesTable.grantReadWriteData(fetchFn)
    dataBucket.grantWrite(fetchFn)

    const requestsLayerArn = `arn:aws:lambda:${process.env.AWS_REGION}:770693421928:layer:Klayers-python38-requests-html:37`
    const requestsLayer = lambda.LayerVersion.fromLayerVersionArn(this, 'fnLayerRequests', requestsLayerArn)
    const yamlLayerArn = `arn:aws:lambda:${process.env.AWS_REGION}:770693421928:layer:Klayers-python38-PyYAML:4`
    const yamlLayer = lambda.LayerVersion.fromLayerVersionArn(this, 'fnLayerYAML', yamlLayerArn)
    
    fetchFn.addLayers(requestsLayer, yamlLayer)
    new DAPScheduledFetchEvents(this, 'scheduledFetch_Events', fetchFn)

    const invokeFetchFn = new lambda.Function(this, 'fnInvokeFetch', {
      handler: 'invoke.lambda_handler',
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

    const publicSubnet = ecsCluster.vpc.publicSubnets[0].subnetId

    const runTaskFn = new lambda.Function(this, 'fnRunTask', {
      handler: 'run_task.lambda_handler',
      runtime: lambda.Runtime.PYTHON_3_8,
      code: lambda.Code.fromAsset('../src/run_task'),
      environment: {
        "TASK_DEFINITION": fetchFargate.taskDefinition.taskDefinitionArn,
        "CONTAINER_NAME": fetchFargate.container.containerName,
        "CLUSTER_NAME": ecsCluster.clusterName,
        "SUBNET_ID": publicSubnet
      },
      memorySize: 128,
      timeout: Duration.seconds(10)
    })

    const grantEcsRunTask = new iam.PolicyStatement();
    grantEcsRunTask.addActions('ecs:RunTask')
    grantEcsRunTask.addResources(fetchFargate.taskDefinition.taskDefinitionArn)

    const taskGrantPassRole = new iam.PolicyStatement();
    taskGrantPassRole.addActions('iam:PassRole')
    taskGrantPassRole.addResources(fetchFargate.taskDefinition.taskRole.roleArn)

    const execGrantPassRole = new iam.PolicyStatement();
    execGrantPassRole.addActions('iam:PassRole')
    execGrantPassRole.addResources(fetchFargate.taskDefinition.executionRole!.roleArn)

    runTaskFn.addToRolePolicy(grantEcsRunTask)
    runTaskFn.addToRolePolicy(taskGrantPassRole)
    runTaskFn.addToRolePolicy(execGrantPassRole)

    runTaskFn.grantInvoke(fetchFn)
    fetchFn.addEnvironment("RUN_TASK_FUNCTION", runTaskFn.functionName)

  }
}
