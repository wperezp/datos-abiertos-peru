import { Construct } from "@aws-cdk/core";
import { FargateTaskDefinition, ContainerImage, LogDriver } from '@aws-cdk/aws-ecs'


export class DAPFetchContainer extends Construct {

  readonly taskDefinition: FargateTaskDefinition;

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
  }
}