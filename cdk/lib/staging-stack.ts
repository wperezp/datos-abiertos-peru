import { Bucket } from "@aws-cdk/aws-s3";
import { Construct, Duration, Stack, StackProps } from "@aws-cdk/core";
import * as lambda from "@aws-cdk/aws-lambda";
import * as s3 from '@aws-cdk/aws-s3';
import { S3EventSource } from '@aws-cdk/aws-lambda-event-sources';

export class DAPStagingStack extends Stack {

  readonly fnCleaning: lambda.Function;

  constructor(scope: Construct, id: string, sourceDataBucket: Bucket, props?: StackProps) {
    super(scope, id, props);

    this.fnCleaning = new lambda.DockerImageFunction(this, "fnCleaning",{
      code: lambda.DockerImageCode.fromImageAsset('../src/staging/'),
      timeout: Duration.minutes(15),
      environment: {
        S3_SOURCE_BUCKET: sourceDataBucket.bucketName
      }
    });

    this.fnCleaning.addEventSource(new S3EventSource(sourceDataBucket, {
      events: [s3.EventType.OBJECT_CREATED],
      filters: [{prefix: 'raw/'}]
    }));

  }
}