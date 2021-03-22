#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { DatosAbiertosPeruStack } from '../lib/datos-abiertos-peru-stack';

const AWS_REGION = 'us-east-2'
const PROJECT_TAG = 'datos-abiertos-peru'

const app = new cdk.App();

new DatosAbiertosPeruStack(app, 'DatosAbiertosPeruStack', {
  env: {
    region: AWS_REGION
  },
  tags: {
    project: PROJECT_TAG
  }
});
