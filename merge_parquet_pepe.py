import pandas as pd

# Cargar el archivo de las estaciones
df_estaciones = pd.read_parquet('id_estaciones.parquet')

# Cargar el archivo que creamos anteriormente
df_datos = pd.read_parquet('tasas_secado.parquet')

print(df_estaciones.columns)
print(df_datos.columns)

print(df_estaciones['id'].dtype)
print(df_datos['id'].dtype)

df_estaciones['id'] = df_estaciones['id'].astype('int64')

df_final = pd.merge(df_estaciones, df_datos, on='id')
df_final = df_final[['id', 'id_celda', 'cap_total', 'ts_hasta_cc', 'ts_tras_cc', 'ts_tras_pm']]

df_final.to_parquet('estaciones_tasas_secado.parquet', index=False)
'''
# Fusionar los dos DataFrames utilizando la columna "id"
df_final = pd.merge(df_estaciones, df_datos, on='id')

# Reorganizar las columnas para poner "id_celda" en segundo lugar
df_final = df_final[['id', 'id_celda', 'cap_total', 'ts_hasta_cc', 'ts_tras_cc', 'ts_tras_pm']]

print(df_final)
'''