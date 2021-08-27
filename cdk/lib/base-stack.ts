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
  readonly fnPrepareFetch: lambda.Function;
  readonly fnRunFetchTask: lambda.Function;
  readonly fnInvokeAll: lambda.Function;
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

    this.fnPrepareFetch = new lambda.Function(this, "fnPrepareFetch", {
      handler: "prepare_fetch.lambda_handler",
      runtime: lambda.Runtime.PYTHON_3_8,
      code: lambda.Code.fromAsset("../src/fetch/prepare"),
      memorySize: 200,
      timeout: Duration.minutes(1)
    })

    // Container to execute when download time is longer than fnFetch timeout
    const fetchContainer = new DAPFetchContainer(this, 'fetchContainer', this.vpc, this.sourceDataBucket, this.hashesTable);

    const requestsLayerArn = `arn:aws:lambda:${process.env.AWS_DEFAULT_REGION}:770693421928:layer:Klayers-python38-requests:20`;
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

    this.fnPrepareFetch.addLayers(requestsLayer);
    this.fnInvokeAll.addLayers(yamlLayer);

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
        "--provisioning_bucket": this.provisioningDataBucket.bucketName,
        "--staging_db": "dap-staging-data",
        "--provisioning_db": "dap-provisioning-data"
      }
    })

    // Workflow
    const sfn_stmxn = new DAPWorkflow(this, 'SfnWorkflow', this.fnPrepareFetch, fetchContainer, this.fnStaging, this.provisioningJob, this.provisioningDataBucket);

    this.fnInvokeAll = new lambda.Function(this, "fnInvokeAll", {
      handler: "invoke_all.lambda_handler",
      runtime: lambda.Runtime.PYTHON_3_8,
      code: lambda.Code.fromAsset("../src/fetch/invoke_all"),
      environment: {
        SFN_STMXN_ARN: sfn_stmxn.workflowStateMachine.stateMachineArn,
      },
      memorySize: 200,
      timeout: Duration.minutes(1)
    });

    this.fnPrepareFetch.grantInvoke(this.fnInvokeAll)

    // Outputs
    new CfnOutput(this, 'SourceBucket', {
      value: this.sourceDataBucket.bucketName
    });
    new CfnOutput(this, 'ProvisioningBucket', {
      value: this.provisioningDataBucket.bucketName
    });
    
  }
}
