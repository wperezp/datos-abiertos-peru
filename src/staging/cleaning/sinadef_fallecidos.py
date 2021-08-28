import io
import pandas as pd


def clean(data: bytes) -> pd.DataFrame:
    data_io = io.BytesIO(data)
    df_sf = pd.read_csv(data_io, sep='|', skiprows=1, header=1, encoding='iso-8859-1', dtype=str)
    df_sf['PAIS DOMICILIO'] = df_sf['PAIS DOMICILIO'].str.strip()
    df_sf['FECHA'] = pd.to_datetime(df_sf['FECHA'], format='%Y-%m-%d')
    df_sf = df_sf.loc[:, ~df_sf.columns.str.contains('^Unnamed')]
    return df_sf
