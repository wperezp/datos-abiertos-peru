import * as sfn from '@aws-cdk/aws-stepfunctions';
import * as tasks from '@aws-cdk/aws-stepfunctions-tasks';
import * as lambda from '@aws-cdk/aws-lambda';
import * as yaml from 'js-yaml';
import * as fs from 'fs';
import SourceDescription from '../utils/source-descriptor';
import { Construct } from '@aws-cdk/core';
import * as targets from '@aws-cdk/aws-events-targets';
import { Rule, RuleTargetInput, Schedule } from '@aws-cdk/aws-events';
import { DAPFetchContainer } from './fetch-container';

export class DAPWorkflow extends Construct {
  readonly workflowStateMachine: sfn.StateMachine;

  constructor(scope: Construct, id: string, fnFetch: lambda.Function, fetchContainer: DAPFetchContainer, fnStaging: lambda.Function) {
    super(scope, id);

    const fetchAsset = new tasks.LambdaInvoke(this, 'FetchAsset', {
      lambdaFunction: fnFetch,
      outputPath: '$.Payload'
    });

    const runTask = new tasks.EcsRunTask(this, 'RunFargate', {
      cluster: fetchContainer.cluster,
      taskDefinition: fetchContainer.taskDefinition,
      integrationPattern: sfn.IntegrationPattern.RUN_JOB,
      launchTarget: new tasks.EcsFargateLaunchTarget(),
      assignPublicIp: true,
      containerOverrides: [
        {
          containerDefinition: fetchContainer.containerDefinition,
          environment: [
            {name: 'ASSET_NAME', value: sfn.JsonPath.stringAt('$.Payload.asset_name')},
            {name: 'ASSET_FILENAME', value: sfn.JsonPath.stringAt('$.Payload.asset_name')},
            {name: 'ASSET_URL', value: sfn.JsonPath.stringAt('$.Payload.asset_name')},
            {name: 'UPLOAD_ONLY_ONCE', value: sfn.JsonPath.stringAt('$.Payload.upload_only_once')},
            {name: 'EXEC_MODE', value: 'FARGATE'}
          ]
        }
      ]
    })

    const stagingJob = new tasks.LambdaInvoke(this, 'Staging', {
      lambdaFunction: fnStaging,
      inputPath: '$.Payload',
      outputPath: '$.Payload'
    })

    const definition = fetchAsset
      .next(new sfn.Choice(this, 'FetchFinished?')
        .when(sfn.Condition.booleanEquals('$.Payload.fetch_finished', false), runTask)
        .afterwards()
      )
      .next(stagingJob);
    
    this.workflowStateMachine = new sfn.StateMachine(this, 'StateMachine', {
      definition: definition
    })

    this.scheduleWorkflowFromCatalog();

  }

  private scheduleWorkflowFromCatalog() {
    let fileContents = fs.readFileSync('../src/fetch/catalog.yml', 'utf-8');
    let sourcesCatalog = yaml.load(fileContents) as SourceDescription;

    for (let [_, item] of Object.entries(sourcesCatalog)) {
      let itemDescription = item as SourceDescription;
      if (itemDescription.hasOwnProperty('CronExpression')) {
        let customPayload = {
          asset_name: itemDescription.Name,
          asset_url: itemDescription.URI,
          asset_filename: itemDescription.Filename,
          cron_expression: itemDescription.CronExpression
        };
        let eventTarget = new targets.SfnStateMachine(this.workflowStateMachine, {
          input: RuleTargetInput.fromObject(customPayload)
        })
  
        new Rule(this, `trigger_${itemDescription.Name}`, {
          schedule: Schedule.expression(`cron(${itemDescription.CronExpression})`),
          targets: [eventTarget]
        });
      }
    }
  }
}