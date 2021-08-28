import * as sfn from '@aws-cdk/aws-stepfunctions';
import * as tasks from '@aws-cdk/aws-stepfunctions-tasks';
import * as lambda from '@aws-cdk/aws-lambda';
import * as yaml from 'js-yaml';
import * as fs from 'fs';
import * as glue from "@aws-cdk/aws-glue";
import * as targets from '@aws-cdk/aws-events-targets';
import * as s3 from "@aws-cdk/aws-s3"
import { Construct } from '@aws-cdk/core';
import { Rule, RuleTargetInput, Schedule } from '@aws-cdk/aws-events';
import { DAPFetchContainer } from './fetch-container';
import SourceDescription from '../utils/source-descriptor';
import { TaskInput } from '@aws-cdk/aws-stepfunctions';

export class DAPWorkflow extends Construct {
  readonly workflowStateMachine: sfn.StateMachine;

  constructor(scope: Construct, id: string, fnPrepareFetch: lambda.Function, fetchContainer: DAPFetchContainer, fnStaging: lambda.Function,
    provisioningJob: glue.CfnJob, provisioningBucket: s3.Bucket) {
    super(scope, id);

    const prepareFetch = new tasks.LambdaInvoke(this, 'PrepareFetch', {
      lambdaFunction: fnPrepareFetch,
      payloadResponseOnly: true
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
          memoryLimit: sfn.JsonPath.numberAt('$.fetch_container_memory'),
          environment: [
            {name: 'ASSET_NAME', value: sfn.JsonPath.stringAt('$.asset_name')},
            {name: 'ASSET_FILENAME', value: sfn.JsonPath.stringAt('$.asset_filename')},
            {name: 'ASSET_URL', value: sfn.JsonPath.stringAt('$.asset_url')},
            {name: 'CRON_EXPRESSION', value: sfn.JsonPath.stringAt('$.cron_expression')},
            {name: 'SIZE_IN_MB', value: sfn.JsonPath.stringAt('$.size_in_mb')},
            {name: 'EXEC_MODE', value: 'FARGATE'}
          ]
        }
      ]
    })

    const stagingJob = new tasks.LambdaInvoke(this, 'Staging', {
      lambdaFunction: fnStaging
    })


    const provisioningGlueJob = new tasks.GlueStartJobRun(this, 'Provisioning', {
      glueJobName: provisioningJob.name!,
      arguments: TaskInput.fromObject({
        "--scriptLocation": sfn.JsonPath.stringAt('$.Payload.asset_etl_script'),
        "--enable-s3-parquet-optimized-committer": "true",
        "--enable-glue-datacatalog": "true",
        "--enable-rename-algorithm-v2": "true",
        "--enable-continuous-cloudwatch-log": "true",
        "--enable-spark-ui": "true",
        "--provisioning_bucket": provisioningBucket.bucketName,
        "--staging_db": "dap-staging-data",
        "--provisioning_db": "dap-provisioning-data"
      }),

    })

    const definition = prepareFetch
      .next(runTask)
      .next(stagingJob)
      .next(new sfn.Choice(this, 'StagingStepWasFound?')
        .when(sfn.Condition.booleanEquals('$.staging_done', true), new sfn.Pass(this, 'ContinueToProvisioningStep'))
        .otherwise(new sfn.Succeed(this, 'NotYetImplemented'))
        .afterwards()
      )
      .next(provisioningGlueJob);
    
    this.workflowStateMachine = new sfn.StateMachine(this, 'StateMachine', {
      definition: definition
    })

    this.scheduleWorkflowFromCatalog();

  }

  private scheduleWorkflowFromCatalog() {
    let fileContents = fs.readFileSync('../src/fetch/fetch/catalog.yml', 'utf-8');
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