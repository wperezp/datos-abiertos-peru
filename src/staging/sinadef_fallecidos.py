import io
import sys
from datetime import datetime, timezone, timedelta

import pandas as pd
import boto3

from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['source_bucket'])
source_bucket = args['source_bucket']
s3 = boto3.client('s3')

asset_obj = s3.get_object(Bucket=source_bucket, Key='raw/sinadef_fallecidos.csv')
data_io = io.BytesIO(asset_obj['Body'].read())

df_sf = pd.read_csv(data_io, sep='|', dtype=str)
df_sf['PAIS DOMICILIO'] = df_sf['PAIS DOMICILIO'].str.strip()
df_sf['FECHA'] = pd.to_datetime(df_sf['FECHA'], format='%Y-%m-%d')
df_clean = df_sf.loc[:, ~df_sf.columns.str.contains('^Unnamed')]

s3_out_key = "s3://{0}/staging/sinadef_fallecidos/sinadef_fallecidos.csv".format(source_bucket)
df_clean.to_csv(s3_out_key, sep=';', index=False)

tz_offset = -5.0  # Lima time (UTC-05:00)
tzinfo = timezone(timedelta(hours=tz_offset))
now = datetime.now(tzinfo)
s3_archive_key = "s3://{0}/archive/sinadef_fallecidos/{1}.csv".format(source_bucket, now.strftime('%Y%m%d%H%M%S'))
