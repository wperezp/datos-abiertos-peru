import * as dynamodb from "@aws-cdk/aws-dynamodb";
import * as s3 from "@aws-cdk/aws-s3";
import * as ec2 from "@aws-cdk/aws-ec2";
import { Construct, Stack, StackProps } from "@aws-cdk/core";

export class DAPBaseStack extends Stack {
  
  readonly sourceDataBucket: s3.Bucket;
  readonly provisioningDataBucket: s3.Bucket;
  readonly hashesTable: dynamodb.Table;
  readonly vpc: ec2.Vpc;

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
  }
}
