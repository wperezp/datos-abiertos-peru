import { Construct, Stack, StackProps } from "@aws-cdk/core";
import { Function } from '@aws-cdk/aws-lambda';
import { LambdaFunction } from '@aws-cdk/aws-events-targets';
import { Rule, RuleTargetInput, Schedule } from "@aws-cdk/aws-events";
import * as yaml from 'js-yaml';
import * as fs from 'fs';


interface SourceDescription {
  Name: string,
  URI: string,
  Filename: string,
  CronExpression?: string
}

function readSourcesCatalog(): object {
  let fileContents = fs.readFileSync('../src/fetch/catalog.yml', 'utf-8');
  let sourcesCatalogYaml = yaml.load(fileContents);
  return typeof sourcesCatalogYaml == 'object' ? sourcesCatalogYaml : Object()
}

export class DAPScheduledFetchEventsStack extends Stack {

  constructor(scope: Construct, id: string, targetFn: Function, props?: StackProps) {
    super(scope, id, props);
    let sourcesCatalog = readSourcesCatalog();

    for (let [_, item] of Object.entries(sourcesCatalog)) {
      let itemDescription = item as SourceDescription;
      if (itemDescription.hasOwnProperty('CronExpression')) {
        let customPayload = {
          asset_name: itemDescription.Name,
          asset_url: itemDescription.URI,
          asset_filename: itemDescription.Filename,
          cron_expression: itemDescription.CronExpression
        };
        let eventTarget = new LambdaFunction(targetFn, {
          event: RuleTargetInput.fromObject(customPayload)
        });
  
        new Rule(this, `trigger_${itemDescription.Name}`, {
          schedule: Schedule.expression(`cron(${itemDescription.CronExpression})`),
          targets: [eventTarget]
        });
      }
    }
  }
}