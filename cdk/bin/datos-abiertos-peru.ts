#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import * as s3n from "@aws-cdk/aws-s3-notifications";
import { DAPBaseStack } from '../lib/base-stack';
import { DAPFetchStack } from '../lib/fetch-stack';
import { DAPScheduledFetchEventsStack } from '../lib/fetch-events-stack';


const AWS_REGION = process.env.AWS_DEFAULT_REGION;
const PROJECT_TAG = 'datos-abiertos-peru';

const app = new cdk.App();
const props = {tags: {project: PROJECT_TAG}, env: {region: AWS_REGION}}

const baseStack = new DAPBaseStack(app, 'DAPBaseStack', props);
const fetchStack = new DAPFetchStack(app, 'DAPFetchStack', baseStack.vpc, baseStack.sourceDataBucket, baseStack.hashesTable, props);
const eventsStack = new DAPScheduledFetchEventsStack(app, 'DAPScheduledFetchEventsStack', fetchStack.fetchFn, props);
