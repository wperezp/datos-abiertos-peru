import boto3
import io
import pandas as pd


def clean(data: bytes) -> pd.DataFrame:
    data_io = io.BytesIO(data)
    df = pd.read_csv(data_io, sep=';', header=0)
    df['FECHA_CORTE'] = pd.to_datetime(df['FECHA_CORTE'], format='%Y%m%d')
    df['FECHA_FALLECIMIENTO'] = pd.to_datetime(df['FECHA_FALLECIMIENTO'], format='%Y%m%d')
    df['FECHA_NAC'] = pd.to_datetime(df['FECHA_NAC'], format='%Y%m%d')
    df.head(10)
    return df
