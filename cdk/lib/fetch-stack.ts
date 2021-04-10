import * as lambda from "@aws-cdk/aws-lambda";
import * as ecs from "@aws-cdk/aws-ecs";
import * as iam from "@aws-cdk/aws-iam";
import { Construct, Duration, Stack, StackProps } from "@aws-cdk/core";
import { Bucket } from "@aws-cdk/aws-s3";
import { Table } from "@aws-cdk/aws-dynamodb";
import { Vpc } from "@aws-cdk/aws-ec2";
import { Cluster, ContainerDefinition, ContainerImage, FargateTaskDefinition, LogDriver } from "@aws-cdk/aws-ecs";

export class DAPFetchStack extends Stack {

  readonly cluster: Cluster;
  readonly taskDefinition: FargateTaskDefinition;
  readonly containerDefinition: ContainerDefinition;
  readonly fetchFn: lambda.Function;
  readonly invokeFetchFn: lambda.Function;
  readonly runTaskFn: lambda.Function;

  constructor(
    scope: Construct,
    id: string,
    vpc: Vpc,
    sourceDataBucket: Bucket,
    hashesTable: Table,
    props?: StackProps,
  ) {
    super(scope, id, props);

    this.fetchFn = new lambda.Function(this, "fnFetch", {
      handler: "fetch.lambda_handler",
      runtime: lambda.Runtime.PYTHON_3_8,
      code: lambda.Code.fromAsset("../src/fetch/"),
      memorySize: 1500,
      timeout: Duration.minutes(15),
      environment: {
        DDB_HASHES_TABLE: hashesTable.tableName,
        S3_DATA_BUCKET: sourceDataBucket.bucketName,
      },
      retryAttempts: 1,
    });

    hashesTable.grantReadWriteData(this.fetchFn);
    sourceDataBucket.grantWrite(this.fetchFn);

    const requestsLayerArn = `arn:aws:lambda:${process.env.AWS_REGION}:770693421928:layer:Klayers-python38-requests-html:37`;
    const requestsLayer = lambda.LayerVersion.fromLayerVersionArn(
      this,
      "fnLayerRequests",
      requestsLayerArn
    );
    const yamlLayerArn = `arn:aws:lambda:${process.env.AWS_REGION}:770693421928:layer:Klayers-python38-PyYAML:4`;
    const yamlLayer = lambda.LayerVersion.fromLayerVersionArn(
      this,
      "fnLayerYAML",
      yamlLayerArn
    );

    this.fetchFn.addLayers(requestsLayer, yamlLayer);

    const invokeFetchFn = new lambda.Function(this, "fnInvokeFetch", {
      handler: "invoke.lambda_handler",
      runtime: lambda.Runtime.PYTHON_3_8,
      code: lambda.Code.fromAsset("../src/invoke"),
      environment: {
        FETCH_FUNCTION_NAME: this.fetchFn.functionName,
      },
      memorySize: 200,
      timeout: Duration.minutes(1),
    });

    invokeFetchFn.addLayers(yamlLayer);

    this.fetchFn.grantInvoke(invokeFetchFn);

    this.cluster = new ecs.Cluster(this, "ecsCluster", {
      containerInsights: true,
      vpc: vpc,
    });

    this.taskDefinition = new FargateTaskDefinition(this, 'FargateTask', {
      cpu: 512,
      memoryLimitMiB: 4096
    });

    this.containerDefinition = this.taskDefinition.addContainer('Container', {
      image: ContainerImage.fromAsset('../src/fetch/'),
      logging: LogDriver.awsLogs({
        streamPrefix: 'Container'
      }),
      environment: {
        S3_DATA_BUCKET: sourceDataBucket.bucketName,
        DDB_HASHES_TABLE: hashesTable.tableName,
        EXEC_MODE: "FARGATE",
      },
      cpu: 512,
      memoryLimitMiB: 4096
    });

    const publicSubnet = vpc.publicSubnets[0].subnetId;

    this.runTaskFn = new lambda.Function(this, "fnRunTask", {
      handler: "run_task.lambda_handler",
      runtime: lambda.Runtime.PYTHON_3_8,
      code: lambda.Code.fromAsset("../src/run_task"),
      environment: {
        TASK_DEFINITION: this.taskDefinition.taskDefinitionArn,
        CONTAINER_NAME: this.containerDefinition.containerName,
        CLUSTER_NAME: this.cluster.clusterName,
        SUBNET_ID: publicSubnet,
      },
      memorySize: 128,
      timeout: Duration.seconds(10),
    });

    const grantEcsRunTask = new iam.PolicyStatement();
    grantEcsRunTask.addActions("ecs:RunTask");
    grantEcsRunTask.addResources(this.taskDefinition.taskDefinitionArn);

    const taskGrantPassRole = new iam.PolicyStatement();
    taskGrantPassRole.addActions("iam:PassRole");
    taskGrantPassRole.addResources(
      this.taskDefinition.taskRole.roleArn
    );

    const execGrantPassRole = new iam.PolicyStatement();
    execGrantPassRole.addActions("iam:PassRole");
    execGrantPassRole.addResources(
      this.taskDefinition.executionRole!.roleArn
    );

    this.runTaskFn.addToRolePolicy(grantEcsRunTask);
    this.runTaskFn.addToRolePolicy(taskGrantPassRole);
    this.runTaskFn.addToRolePolicy(execGrantPassRole);

    this.runTaskFn.grantInvoke(this.fetchFn);
    this.fetchFn.addEnvironment("RUN_TASK_FUNCTION", this.runTaskFn.functionName);
  }
}
