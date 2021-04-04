import { Construct } from "@aws-cdk/core";
import { FargateTaskDefinition, ContainerImage, Cluster, LogDriver } from '@aws-cdk/aws-ecs'
import { RunTask } from 'cdk-fargate-run-task'
import { Repository } from '@aws-cdk/aws-ecr'
import { DockerImageAsset } from '@aws-cdk/aws-ecr-assets';


export class DAPFetchContainer extends Construct {

  readonly taskDefinition: FargateTaskDefinition;
  readonly cluster: Cluster;

  constructor(scope: Construct, id: string, containerEnv = {}) {
    super(scope, id);
    this.taskDefinition = new FargateTaskDefinition(this, 'Task', {
      cpu: 512,
      memoryLimitMiB: 4096
    });

    this.taskDefinition.addContainer('Container', {
      image: ContainerImage.fromAsset('../src/fetch/'),
      logging: LogDriver.awsLogs({
        streamPrefix: 'Container'
      }),
      environment: containerEnv,
      cpu: 512,
      memoryLimitMiB: 4096
    });

    this.cluster = new Cluster(this, 'Cluster', { containerInsights: true })
    // const runTaskOnce = new RunTask(this, 'runOnce', {
    //   task: this.taskDefinition,
    //   cluster: sfCluster,
    //   runOnResourceUpdate: true
    // })
  }
}