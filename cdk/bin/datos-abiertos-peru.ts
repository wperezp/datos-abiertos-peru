#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from '@aws-cdk/core';
import { DatosAbiertosPeruStack } from '../lib/datos-abiertos-peru-stack';

const app = new cdk.App();
new DatosAbiertosPeruStack(app, 'DatosAbiertosPeruStack', { env: { region: 'us-east-1' }, tags: { project: 'datos-abiertos' } });
