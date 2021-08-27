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
    sourceDataBucket: Bucket,
    hashesTable: Table
  ) {
    super(scope, id);

    this.cluster = new ecs.Cluster(this, "ecsCluster", {
      containerInsights: true,
      vpc: vpc,
    });

    this.taskDefinition = new FargateTaskDefinition(this, 'FargateTask', {
      cpu: 1024,
      memoryLimitMiB: 8192
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


    // this.runTaskFn.grantInvoke(fnFetch);
    // fnFetch.addEnvironment("RUN_TASK_FUNCTION", this.runTaskFn.functionName);
  }
}
