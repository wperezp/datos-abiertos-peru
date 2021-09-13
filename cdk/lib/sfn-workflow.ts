import * as sfn from '@aws-cdk/aws-stepfunctions';
import * as tasks from '@aws-cdk/aws-stepfunctions-tasks';
import * as lambda from '@aws-cdk/aws-lambda';
import * as glue from "@aws-cdk/aws-glue";

import { Construct } from '@aws-cdk/core';
import { DAPFetchContainer } from './fetch-container';
import SourceDescription from '../utils/source-descriptor';

import { TaskInput } from '@aws-cdk/aws-stepfunctions';

export class DAPWorkflow extends Construct {
  readonly workflowStateMachine: sfn.StateMachine;

  constructor(scope: Construct, 
    id: string, 
    itemDescription: SourceDescription,
    fnPrepareFetch: lambda.Function, 
    fetchContainer: DAPFetchContainer,
    stagingJob?: glue.CfnJob,
    provisioningJob?: glue.CfnJob ) 
  {
    super(scope, id);

    const prepareFetch = new tasks.LambdaInvoke(this, 'PrepareFetch', {
      lambdaFunction: fnPrepareFetch,
      payload: TaskInput.fromObject({
        asset_name: itemDescription.Name,
        asset_url: itemDescription.URI,
        asset_filename: itemDescription.Filename,
        cron_expression: itemDescription.CronExpression
      }),
      payloadResponseOnly: true
    });

    const runTask = new tasks.EcsRunTask(this, 'RunFetch', {
      cluster: fetchContainer.cluster,
      taskDefinition: fetchContainer.taskDefinition,
      integrationPattern: sfn.IntegrationPattern.RUN_JOB,
      launchTarget: new tasks.EcsFargateLaunchTarget(),
      assignPublicIp: true,
      containerOverrides: [
        {
          containerDefinition: fetchContainer.containerDefinition,
          memoryLimit: sfn.JsonPath.numberAt('$.fetch_container_memory'),
          environment: [
            {name: 'ASSET_NAME', value: sfn.JsonPath.stringAt('$.asset_name')},
            {name: 'ASSET_FILENAME', value: sfn.JsonPath.stringAt('$.asset_filename')},
            {name: 'ASSET_URL', value: sfn.JsonPath.stringAt('$.asset_url')},
            {name: 'CRON_EXPRESSION', value: sfn.JsonPath.stringAt('$.cron_expression')},
            {name: 'SIZE_IN_MB', value: sfn.JsonPath.stringAt('$.size_in_mb')},
            {name: 'STAGING_DPUS', value: sfn.JsonPath.stringAt('$.staging_dpus')},
            {name: 'PROVISIONING_DPUS', value: sfn.JsonPath.stringAt('$.provisioning_dpus')},
            {name: 'EXEC_MODE', value: 'FARGATE'}
          ]
        }
      ]
    })

    let definition = prepareFetch.next(runTask)

    if (stagingJob !== undefined) {
      const stagingJobTask = new tasks.GlueStartJobRun(this, 'Staging', {
        glueJobName: stagingJob!.name!
      })
      definition = definition.next(stagingJobTask)
    }

    if (provisioningJob !== undefined) {
      const provisioningJobTask = new tasks.GlueStartJobRun(this, 'Provisioning', {
        glueJobName: provisioningJob!.name!
      })
      definition = definition.next(provisioningJobTask)
    }
    
    this.workflowStateMachine = new sfn.StateMachine(this, 'SM', {
      definition: definition
    })

  }
}