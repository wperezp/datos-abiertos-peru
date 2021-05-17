import * as dynamodb from "@aws-cdk/aws-dynamodb";
import * as s3 from "@aws-cdk/aws-s3";
import * as ec2 from "@aws-cdk/aws-ec2";
import * as lambda from "@aws-cdk/aws-lambda";
import * as glue from "@aws-cdk/aws-glue";
import * as iam from "@aws-cdk/aws-iam";
import { CfnOutput, Construct, Duration, Stack, StackProps } from "@aws-cdk/core";
import { DAPFetchContainer } from "./fetch-container";
import { DAPWorkflow } from "./sfn-workflow";
import { AnyPrincipal, ServicePrincipal } from "@aws-cdk/aws-iam";

export class DAPBaseStack extends Stack {
  
  readonly vpc: ec2.Vpc;
  readonly sourceDataBucket: s3.Bucket;
  readonly provisioningDataBucket: s3.Bucket;
  readonly hashesTable: dynamodb.Table;
  readonly fnFetch: lambda.Function;
  readonly fnInvokeFetch: lambda.Function;
  readonly fnStaging: lambda.Function;
  readonly provisioningJob: glue.CfnJob;

  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    this.sourceDataBucket = new s3.Bucket(this, "sourceData", {
      versioned: false,
    });

    this.provisioningDataBucket = new s3.Bucket(this, "provisionData", {
      versioned: true,
    });

    this.hashesTable = new dynamodb.Table(this, "md5Hashes", {
      partitionKey: {
        name: "asset_name",
        type: dynamodb.AttributeType.STRING,
      },
      readCapacity: 1,
      writeCapacity: 1,
    });

    this.vpc = new ec2.Vpc(this, "VPC", {
      maxAzs: 2,
      subnetConfiguration: [
        {
          name: "default",
          subnetType: ec2.SubnetType.PUBLIC,
        },
      ],
    });


    // Fetch function
    this.fnFetch = new lambda.Function(this, "fnFetch", {
      handler: "fetch.lambda_handler",
      runtime: lambda.Runtime.PYTHON_3_8,
      code: lambda.Code.fromAsset("../src/fetch/"),
      memorySize: 1500,
      timeout: Duration.minutes(15),
      environment: {
        DDB_HASHES_TABLE: this.hashesTable.tableName,
        S3_DATA_BUCKET: this.sourceDataBucket.bucketName,
      },
      retryAttempts: 1,
    });

    this.fnInvokeFetch = new lambda.Function(this, "fnInvokeFetch", {
      handler: "invoke.lambda_handler",
      runtime: lambda.Runtime.PYTHON_3_8,
      code: lambda.Code.fromAsset("../src/invoke"),
      environment: {
        FETCH_FUNCTION_NAME: this.fnFetch.functionName,
      },
      memorySize: 200,
      timeout: Duration.minutes(1),
    });

    this.hashesTable.grantReadWriteData(this.fnFetch);
    this.sourceDataBucket.grantWrite(this.fnFetch);

    const requestsLayerArn = `arn:aws:lambda:${process.env.AWS_DEFAULT_REGION}:770693421928:layer:Klayers-python38-requests-html:37`;
    const requestsLayer = lambda.LayerVersion.fromLayerVersionArn(
      this,
      "fnLayerRequests",
      requestsLayerArn
    );
    const yamlLayerArn = `arn:aws:lambda:${process.env.AWS_DEFAULT_REGION}:770693421928:layer:Klayers-python38-PyYAML:4`;
    const yamlLayer = lambda.LayerVersion.fromLayerVersionArn(
      this,
      "fnLayerYAML",
      yamlLayerArn
    );

    this.fnFetch.addLayers(requestsLayer, yamlLayer);
    this.fnInvokeFetch.addLayers(yamlLayer);

    this.fnFetch.grantInvoke(this.fnInvokeFetch);

    // Container to execute when download time is longer than fnFetch timeout
    const fetchContainer = new DAPFetchContainer(this, 'fetchContainer', this.vpc, this.fnFetch, this.sourceDataBucket, this.hashesTable);

    // Staging function
    this.fnStaging = new lambda.DockerImageFunction(this, "fnCleaning",{
      code: lambda.DockerImageCode.fromImageAsset('../src/staging/'),
      timeout: Duration.minutes(15),
      environment: {
        S3_SOURCE_BUCKET: this.sourceDataBucket.bucketName,
        S3_PROVISIONING_BUCKET: this.provisioningDataBucket.bucketName
      },
      memorySize: 4096
    });
    
    this.sourceDataBucket.grantReadWrite(this.fnStaging);

    // const provisioningGluePolicy = new iam.PolicyStatement();
    // provisioningGluePolicy.addActions("s3:*")
    // provisioningGluePolicy.addResources(this.sourceDataBucket.bucketArn, this.provisioningDataBucket.bucketArn);

    const provisioningGlueRole = new iam.Role(this, 'prvRole', {
      assumedBy: new ServicePrincipal('glue.amazonaws.com')
    })

    this.sourceDataBucket.grantReadWrite(provisioningGlueRole);
    this.provisioningDataBucket.grantReadWrite(provisioningGlueRole);
    provisioningGlueRole.addManagedPolicy(iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'))

    this.provisioningJob = new glue.CfnJob(this, 'prvJob', {
      command: {
        name: 'glueetl',
        pythonVersion: '3',
        scriptLocation: `s3://${this.provisioningDataBucket.bucketName}/scripts/default.py`
      },
      role: provisioningGlueRole.roleArn,
      glueVersion: '2.0',
      executionProperty: {
        maxConcurrentRuns: 50.0,
      },
      maxCapacity: 2,
      name: 'DAPProvisioningJob',
      defaultArguments: {
        "--TempDir": `s3://${this.provisioningDataBucket.bucketName}/temp/`,
        "--enable-s3-parquet-optimized-committer": true,
        "--enable-glue-datacatalog": true,
        "--enable-rename-algorithm-v2": true,
        "--enable-continuous-cloudwatch-log": true,
        "--enable-spark-ui": true,
        "--provisioning-bucket": this.provisioningDataBucket.bucketName,
        "--staging-db": "dap-staging-data",
        "--provisioning-db": "dap-provisioning-db"
      }
    })

    // Workflow
    new DAPWorkflow(this, 'SfnWorkflow', this.fnFetch, fetchContainer, this.fnStaging, this.provisioningJob, this.provisioningDataBucket);

    // Outputs
    new CfnOutput(this, 'SourceBucket', {
      value: this.sourceDataBucket.bucketName
    });
    new CfnOutput(this, 'ProvisioningBucket', {
      value: this.provisioningDataBucket.bucketName
    });
    
  }
}
