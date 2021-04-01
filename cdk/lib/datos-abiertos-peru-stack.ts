import * as cdk from '@aws-cdk/core';
import * as s3 from '@aws-cdk/aws-s3';
import * as dynamodb from '@aws-cdk/aws-dynamodb'
import * as lambda from '@aws-cdk/aws-lambda'
import { Duration } from '@aws-cdk/core';
import { DAPDailyFetchEvents } from './daily-fetch-events'
import { DAPSingleFetchContainer } from './single-fetch-container';

export class DatosAbiertosPeruStack extends cdk.Stack {
  constructor(scope: cdk.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);
    
    const dataBucket = new s3.Bucket(this, 'source-data', {
      versioned: false
    })

    const hashesTable = new dynamodb.Table(this, 'md5_hashes', {
      partitionKey: {
        name: 'asset_name',
        type: dynamodb.AttributeType.STRING
      },
      readCapacity: 1,
      writeCapacity: 1,
    })

    const fetchFunction = new lambda.Function(this, 'fnDailyFetch', {
      handler: 'daily_fetch.get_dataset',
      runtime: lambda.Runtime.PYTHON_3_8,
      code: lambda.Code.fromAsset('../src/fetch/daily'),
      memorySize: 1024,
      timeout: Duration.minutes(10),
      environment: {
        "DDB_HASHES_TABLE": hashesTable.tableName,
        "S3_DATA_BUCKET": dataBucket.bucketName
      }
    })

    hashesTable.grantReadWriteData(fetchFunction)
    dataBucket.grantWrite(fetchFunction)

    const fetchRequestLayer = lambda.LayerVersion
      .fromLayerVersionArn(this, 'fnLayerRequests', 'arn:aws:lambda:us-east-2:770693421928:layer:Klayers-python38-requests-html:37')
    
    fetchFunction.addLayers(fetchRequestLayer)

    new DAPDailyFetchEvents(this, 'dailyFetch_Events', fetchFunction)
    
    const singleFetchFargate = new DAPSingleFetchContainer(this, 'singleFetch', {
      S3_DATA_BUCKET: dataBucket.bucketName,
      DDB_HASHES_TABLE: hashesTable.tableName
    })

    hashesTable.grantReadWriteData(singleFetchFargate.taskDefinition.taskRole)
    dataBucket.grantWrite(singleFetchFargate.taskDefinition.taskRole)
  }
}
