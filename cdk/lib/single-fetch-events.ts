import { Construct } from "@aws-cdk/core";
import { Function } from '@aws-cdk/aws-lambda';
import { DockerImageAsset } from '@aws-cdk/aws-ecr-assets';
import { Rule, RuleTargetInput, Schedule } from "@aws-cdk/aws-events";
import * as yaml from 'js-yaml';
import * as fs from 'fs';


export class DAPSingleFetchContainer extends Construct {

  constructor(scope: Construct, id: string) {
    super(scope, id);
    let imageAsset = new DockerImageAsset(this, `${id}Image`, {
      directory: '../src/fetch/standalone/',
    })    
  }
}