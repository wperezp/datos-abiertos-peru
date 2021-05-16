from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

db_provisioning = 'dap-provisioning-data'
db_staging = 'dap-staging-data'
tbl_name = 'sinadef_fallecidos'

dyf_staging: DynamicFrame = glueContext.create_dynamic_frame.from_catalog(database=db_staging, table_name=tbl_name)

if tbl_name not in spark.catalog.listTables(db_provisioning):
    glueContext.write_dynamic_frame_from_options(
        frame=dyf_staging,
        connection_type="parquet",
        connection_options={
            "path": f"s3://dapbasestack-provisiondata70827853-dhic82n9go2r/data/{tbl_name}/"
        }
    )
else:
    df_staging: DataFrame = dyf_staging.toDF()
    dyf_provisioning: DynamicFrame = glueContext.create_dynamic_frame.from_catalog(database=db_staging, table_name=tbl_name)
    df_provisioning: DataFrame = dyf_provisioning.toDF()
    df_provisioning.union(df_staging)







# if tbl_name not in [t.name for t in spark.catalog.listTables(db_provisioning)]:
#     spark.catalog.setCurrentDatabase(db_provisioning)
#     spark.catalog.createTable(tbl_name)
#
# df_provisioning = glueContext.create_dynamic_frame.from_catalog(database=db_provisioning, table_name=tbl_name)
#
# df_update = df_provisioning.


