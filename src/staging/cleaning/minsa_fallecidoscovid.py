import io
import pandas as pd


def clean(data: bytes) -> pd.DataFrame:
    data_io = io.BytesIO(data)
    df = pd.read_csv(data_io, sep=';', dtype=str)
    df['FECHA_CORTE'] = pd.to_datetime(df['FECHA_CORTE'], format='%Y%m%d')
    df['FECHA_FALLECIMIENTO'] = pd.to_datetime(df['FECHA_FALLECIMIENTO'], format='%Y%m%d')
    df['CLASIFICACION_DEF'] = [bytes(x.encode('windows-1252')).decode('utf-8') for x in df['CLASIFICACION_DEF']]
    df['PROVINCIA'] = [bytes(x.encode('windows-1252')).decode('utf-8') for x in df['PROVINCIA']]
    df['DISTRITO'] = [bytes(x.encode('windows-1252')).decode('utf-8') for x in df['DISTRITO']]
    return df
