import * as cdk from '@aws-cdk/core';
import * as s3 from '@aws-cdk/aws-s3';
import * as dynamodb from '@aws-cdk/aws-dynamodb'
import * as lambda from '@aws-cdk/aws-lambda'
import { Rule, RuleTargetInput, Schedule } from '@aws-cdk/aws-events'
import { LambdaFunction } from '@aws-cdk/aws-events-targets'
import { Duration } from '@aws-cdk/core';
import { DAPFetchEvents } from './dap-events-construct'

export class DatosAbiertosPeruStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);
    
    const dataBucket = new s3.Bucket(this, 'dap-data', {
      versioned: false
    })

    const hashesTable = new dynamodb.Table(this, 'dap_md5_hashes', {
      partitionKey: {
        name: 'asset_name',
        type: dynamodb.AttributeType.STRING
      },
      readCapacity: 1,
      writeCapacity: 1,
    })

    const fetchFunction = new lambda.Function(this, 'dap_fn_fetch', {
      handler: 'fetch_assets.get_dataset',
      runtime: lambda.Runtime.PYTHON_3_8,
      code: lambda.Code.fromAsset('../src/fetch'),
      memorySize: 512,
      timeout: Duration.minutes(5),
      environment: {
        "DDB_HASHES_TABLE": hashesTable.tableName,
        "S3_DATA_BUCKET": dataBucket.bucketName
      }
    })

    hashesTable.grantReadWriteData(fetchFunction)
    dataBucket.grantWrite(fetchFunction)

    const fetchRequestLayer = lambda.LayerVersion
      .fromLayerVersionArn(this, 'dap_layer_requests', 'arn:aws:lambda:us-east-2:770693421928:layer:Klayers-python38-requests-html:37')
    
    fetchFunction.addLayers(fetchRequestLayer)

    new DAPFetchEvents(this, 'fetch_events', fetchFunction)
  }
}
