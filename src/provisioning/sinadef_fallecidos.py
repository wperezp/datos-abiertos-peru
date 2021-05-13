import sys

from awsglue.transforms import Join, Union
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

db_provisioning = 'dap-provisioning-data'
db_staging = 'dap-staging-data'
tbl_name = 'sinadef_fallecidos'

dyf_staging = glueContext.create_dynamic_frame.from_catalog(database=db_staging, table_name=tbl_name)
df_staging = dyf_staging.toDF()

print(df_staging.columns)
df_staging.show(10)


# if tbl_name not in [t.name for t in spark.catalog.listTables(db_provisioning)]:
#     spark.catalog.setCurrentDatabase(db_provisioning)
#     spark.catalog.createTable(tbl_name)
#
# df_provisioning = glueContext.create_dynamic_frame.from_catalog(database=db_provisioning, table_name=tbl_name)
#
# df_update = df_provisioning.


