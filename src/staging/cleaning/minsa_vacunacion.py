import io
import pandas as pd


def clean(data: bytes) -> pd.DataFrame:
    data_io = io.BytesIO(data)
    df = pd.read_csv(data_io, sep=';', dtype=str)
    df = df.fillna('NULL')
    df['FECHA_CORTE'] = pd.to_datetime(df['FECHA_CORTE'], format='%Y%m%d')
    df['FECHA_VACUNACION'] = pd.to_datetime(df['FECHA_VACUNACION'], format='%Y%m%d')
    return df