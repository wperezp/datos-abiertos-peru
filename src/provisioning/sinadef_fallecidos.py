import sys

from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions


args = getResolvedOptions(sys.argv, ['provisioning_bucket', 'staging_db', 'provisioning_db'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

db_staging = args['staging_db']
db_provisioning = args['provisioning_db']
provisioning_bucket = args['provisioning_bucket']
tbl_name = 'sinadef_fallecidos'


dyf_staging: DynamicFrame = glueContext.create_dynamic_frame.from_catalog(database=db_staging, table_name=tbl_name)

dyf_staging = dyf_staging.applyMapping(mappings=[
    ('tipo seguro', 'string', 'tipo_seguro', 'string'),
    ('sexo', 'string', 'sexo', 'string'),
    ('edad', 'string', 'edad', 'short'),
    ('tiempo edad', 'string', 'tiempo_edad', 'string'),
    ('estado civil', 'string', 'estado_civil', 'string'),
    ('nivel de instrucción', 'string', 'nivel_instruccion', 'string'),
    ('cod# ubigeo domicilio', 'string', 'ubigeo_domicilio', 'string'),
    ('pais domicilio', 'string', 'pais_domicilio', 'string'),
    ('departamento domicilio', 'string', 'departamento_domicilio', 'string'),
    ('provincia domicilio', 'string', 'provincia_domicilio', 'string'),
    ('distrito domicilio', 'string', 'distrito_domicilio', 'string'),
    ('fecha', 'string', 'fecha_fallecimiento', 'date'),
    ('tipo lugar', 'string', 'tipo_lugar', 'string'),
    ('institucion', 'string', 'institucion', 'string'),
    ('muerte violenta', 'string', 'muerte_violenta', 'string'),
    ('necropsia', 'string', 'necropsia', 'string'),
    ('debido a (causa a)', 'string', 'causa_a', 'string'),
    ('debido a (causa b)', 'string', 'causa_b', 'string'),
    ('debido a (causa c)', 'string', 'causa_c', 'string'),
    ('debido a (causa d)', 'string', 'causa_d', 'string'),
    ('debido a (causa e)', 'string', 'causa_e', 'string'),
    ('debido a (causa f)', 'string', 'causa_f', 'string'),
    ('causa a (cie-x)', 'string', 'causa_a_cie', 'string'),
    ('causa b (cie-x)', 'string', 'causa_b_cie', 'string'),
    ('causa c (cie-x)', 'string', 'causa_c_cie', 'string'),
    ('causa d (cie-x)', 'string', 'causa_d_cie', 'string'),
    ('causa e (cie-x)', 'string', 'causa_e_cie', 'string'),
    ('causa f (cie-x)', 'string', 'causa_f_cie', 'string'),
])

df_staging = dyf_staging.toDF()
df_necropsia = df_staging.withColumn('necropsia_bool', when(col('necropsia') == 'NO SE REALIZÓ NECROPSIA', False)
                                     .when(col('necropsia') == 'SI SE REALIZÓ NECROPSIA', True)
                                     .when(col('necropsia') == 'SIN REGISTRO', None))
df_dropped = df_necropsia.drop('necropsia')
df_final = df_dropped.withColumnRenamed('necropsia_bool', 'necropsia')


df_final \
    .write.mode('overwrite').format('parquet').save(f"s3://{provisioning_bucket}/data/{tbl_name}/")