import sys

from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame, DynamicFrameWriter
from awsglue.utils import getResolvedOptions

from pyspark.sql.functions import col, upper

args = getResolvedOptions(sys.argv, ['provisioning_bucket', 'staging_db', 'provisioning_db'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

db_staging = args['staging_db']
db_provisioning = args['provisioning_db']
provisioning_bucket = args['provisioning_bucket']
tbl_name = 'minsa_vacunacion'

dyf_staging: DynamicFrame = glueContext.create_dynamic_frame.from_catalog(database=db_staging, table_name=tbl_name)

dyf_final = dyf_staging.applyMapping(mappings=[
    ('grupo_riesgo', 'string', 'grupo_riesgo', 'string'),
    ('edad', 'string', 'edad', 'short'),
    ('sexo', 'string', 'sexo', 'string'),
    ('fecha_vacunacion', 'string', 'fecha_vacunacion', 'date'),
    ('dosis', 'nro_dosis', 'short'),
    ('fabricante', 'string', 'fabricante', 'string'),
    ('diresa', 'string', 'diresa', 'string'),
    ('departamento', 'string', 'departamento', 'string'),
    ('provincia', 'string', 'provincia', 'string'),
    ('distrito', 'string', 'distrito', 'string')
])

glueContext.write_dynamic_frame.from_options(
    frame=dyf_final,
    connection_type='s3',
    connection_options={
        "path": f"s3://{provisioning_bucket}/data/{tbl_name}/"
    },
    format='glueparquet'
)
