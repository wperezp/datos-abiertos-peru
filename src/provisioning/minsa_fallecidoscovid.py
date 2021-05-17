import sys

from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions


args = getResolvedOptions(sys.argv, ['provisioning_bucket', 'staging_db', 'provisioning_db'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

db_staging = args['provisioning_db']
db_provisioning = args['staging_db']
provisioning_bucket = args['provisioning_bucket']
tbl_name = 'minsa_fallecidoscovid'


dyf_staging: DynamicFrame = glueContext.create_dynamic_frame.from_catalog(database=db_staging, table_name=tbl_name)

dyf_staging = dyf_staging.applyMapping(mappings=[
    ('uuid', 'string', 'uuid_fallecimiento', 'string'),
    ('fecha_fallecimiento', 'string', 'fecha_fallecimiento', 'date'),
    ('edad_declarada', 'string', 'edad', 'short'),
    ('sexo', 'string', 'sexo', 'string'),
    ('fecha_nac', 'string', 'fecha_nacimiento', 'date'),
    ('ubigeo', 'string', 'ubigeo', 'short'),
    ('departamento', 'string', 'departamento', 'string'),
    ('provincia', 'string', 'provincia', 'string'),
    ('distrito', 'string', 'distrito', 'string')
])

prv_tables = [x.name for x in spark.catalog.listTables(db_provisioning)]

if tbl_name not in prv_tables:
    df_final = dyf_staging.toDF()
else:
    df_staging: DataFrame = dyf_staging.toDF()
    dyf_prv = glueContext.create_dynamic_frame_from_catalog(database=db_provisioning, table_name=tbl_name)
    df_prv = dyf_prv.toDF()
    df_union = df_prv.unionAll(df_staging)
    df_union_unique = df_union.dropDuplicates(subset=['uuid_fallecimiento'])
    df_final = df_union_unique

df_final \
    .write.mode('overwrite').format('parquet').save(f"s3://{provisioning_bucket}/data/{tbl_name}/")

