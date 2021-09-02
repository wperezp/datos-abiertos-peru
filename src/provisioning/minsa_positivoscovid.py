import sys

from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame, DynamicFrameWriter
from awsglue.utils import getResolvedOptions

from pyspark.sql.functions import when, col, upper

args = getResolvedOptions(sys.argv, ['provisioning_bucket', 'staging_db', 'provisioning_db'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

db_staging = args['staging_db']
db_provisioning = args['provisioning_db']
provisioning_bucket = args['provisioning_bucket']
tbl_name = 'minsa_positivoscovid'

dyf_staging: DynamicFrame = glueContext.create_dynamic_frame.from_catalog(database=db_staging, table_name=tbl_name)

dyf_typed = dyf_staging.applyMapping(mappings=[
    ('departamento', 'string', 'departamento', 'string'),
    ('provincia', 'string', 'provincia', 'string'),
    ('distrito', 'string', 'distrito', 'string'),
    ('metododx', 'string', 'metodo_dx', 'string'),
    ('edad', 'string', 'edad', 'short'),
    ('sexo', 'string', 'sexo', 'string'),
    ('fecha_resultado', 'string', 'fecha_resultado', 'date'),
    ('ubigeo', 'string', 'ubigeo', 'string')
])

df = dyf_typed.toDF()

df_notnull = df \
                .withColumn('edad_nn', when(col('edad') == '', None).otherwise(col('edad'))) \
                .withColumn('sexo_nn', when(col('sexo') == '', None).otherwise(col('sexo'))) \
                .withColumn('fecha_resultado_nn', when(col('fecha_resultado') == '', None)
                            .otherwise(col('fecha_resultado'))) \
                .withColumn('ubigeo_nn', when(col('ubigeo') == '', None).otherwise(col('ubigeo')))

df_final = df_notnull \
            .drop('edad', 'sexo', 'fecha_resultado', 'ubigeo') \
            .withColumnRenamed('edad_nn', 'edad') \
            .withColumnRenamed('sexo_nn', 'sexo') \
            .withColumnRenamed('fecha_resultado_nn', 'fecha_resultado') \
            .withColumnRenamed('ubigeo_nn', 'ubigeo')

df_final \
    .write.mode('overwrite').format('parquet').save(f"s3://{provisioning_bucket}/data/{tbl_name}/")