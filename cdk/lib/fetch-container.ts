import * as lambda from "@aws-cdk/aws-lambda";
import * as ecs from "@aws-cdk/aws-ecs";
import * as iam from "@aws-cdk/aws-iam";
import { Construct, Duration } from "@aws-cdk/core";
import { Bucket } from "@aws-cdk/aws-s3";
import { Table } from "@aws-cdk/aws-dynamodb";
import { Vpc } from "@aws-cdk/aws-ec2";
import { Cluster, ContainerDefinition, ContainerImage, FargateTaskDefinition, LogDriver } from "@aws-cdk/aws-ecs";

export class DAPFetchContainer extends Construct {

  readonly cluster: Cluster;
  readonly taskDefinition: FargateTaskDefinition;
  readonly containerDefinition: ContainerDefinition;
  readonly invokeFetchFn: lambda.Function;
  readonly runTaskFn: lambda.Function;

  constructor(
    scope: Construct,
    id: string,
    vpc: Vpc,
    fnFetch: lambda.Function,
    sourceDataBucket: Bucket,
    hashesTable: Table
  ) {
    super(scope, id);

    this.cluster = new ecs.Cluster(this, "ecsCluster", {
      containerInsights: true,
      vpc: vpc,
    });

    this.taskDefinition = new FargateTaskDefinition(this, 'FargateTask', {
      cpu: 512,
      memoryLimitMiB: 4096
    });

    hashesTable.grantReadWriteData(this.taskDefinition.taskRole);
    sourceDataBucket.grantWrite(this.taskDefinition.taskRole);

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

    const taskGrantPass = new iam.PolicyStatement();
    taskGrantPass.addActions("iam:PassRole");
    taskGrantPass.addResources(
      this.taskDefinition.taskRole.roleArn
    );

    const execGrantPass = new iam.PolicyStatement();
    execGrantPass.addActions("iam:PassRole");
    execGrantPass.addResources(
      this.taskDefinition.executionRole!.roleArn
    );

    this.runTaskFn.addToRolePolicy(grantEcsRunTask);
    this.runTaskFn.addToRolePolicy(taskGrantPass);
    this.runTaskFn.addToRolePolicy(execGrantPass);

    this.runTaskFn.grantInvoke(fnFetch);
    fnFetch.addEnvironment("RUN_TASK_FUNCTION", this.runTaskFn.functionName);
  }
}
