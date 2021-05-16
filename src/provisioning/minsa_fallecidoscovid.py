import sys

from awsglue.transforms import Join, Union
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

db_provisioning = 'dap-provisioning-data'
db_staging = 'dap-staging-data'
tbl_name = 'minsa_fallecidoscovid'

dyf_staging: DynamicFrame = glueContext.create_dynamic_frame.from_catalog(database=db_staging, table_name=tbl_name)

if tbl_name not in spark.catalog.listTables(db_provisioning):
    glueContext.write_dynamic_frame_from_options(
        frame=dyf_staging,
        connection_type="parquet",
        connection_options={
            "path": f"s3://dapbasestack-provisiondata70827853-dhic82n9go2r/data/{tbl_name}/"
        }
    )

