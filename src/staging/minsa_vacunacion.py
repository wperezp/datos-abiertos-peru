# import io
# import pandas as pd
#
#
# def clean(data: bytes) -> pd.DataFrame:
#     data_io = io.BytesIO(data)
#     df = pd.read_csv(data_io, sep=';', dtype=str)
#     # df = df.fillna('NULL')
#     df['FECHA_CORTE'] = pd.to_datetime(df['FECHA_CORTE'], format='%Y-%m-%d')
#     df['FECHA_VACUNACION'] = pd.to_datetime(df['FECHA_VACUNACION'], format='%Y-%m-%d')
#     return df

import io
import sys
from datetime import datetime, timezone, timedelta

import pandas as pd
import boto3
import py7zr

from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['source_bucket'])
source_bucket = args['source_bucket']
s3 = boto3.client('s3')

asset_obj = s3.get_object(Bucket=source_bucket, Key='raw/minsa_vacunacion.7z')
data_io = io.BytesIO(asset_obj['Body'].read())

with py7zr.SevenZipFile(data_io, 'r') as compressed_file:
    compressed_file.extractall('.')


df = pd.read_csv('TB_VACUNACION_COVID19.csv', sep=';', dtype=str)
df['fecha_vacunacion'] = pd.to_datetime(df['fecha_vacunacion'], format='%d/%m/%Y')
df.to_csv('clean.csv', sep=';', index=False)

del df

s3_out_key = "staging/minsa_vacunacion/minsa_vacunacion.csv"
s3.upload_file('clean.csv', source_bucket, s3_out_key)


tz_offset = -5.0  # Lima time (UTC-05:00)
tzinfo = timezone(timedelta(hours=tz_offset))
now = datetime.now(tzinfo)
s3_archive_key = "archive/minsa_vacunacion/{0}.csv".format(now.strftime('%Y%m%d%H%M%S'))
s3.upload_file('clean.csv', source_bucket, s3_archive_key)