import io
import pandas as pd


def clean(data: bytes) -> pd.DataFrame:
    data_io = io.BytesIO(data)
    df = pd.read_csv(data_io, sep=';', dtype=str)
    df['FECHA_CORTE'] = pd.to_datetime(df['FECHA_CORTE'], format='%Y%m%d')
    df['FECHA_RESULTADO'] = pd.to_datetime(df['FECHA_RESULTADO'], format='%Y%m%d')
    df.loc[:, 'PROVINCIA'] = [bytes(x.encode('latin-1')).decode('utf-8') for x in df['PROVINCIA']]
    df.loc[:, 'DISTRITO'] = [bytes(x.encode('latin-1')).decode('utf-8') for x in df['DISTRITO']]
    return df