import pandas as pd

# Leer el archivo Parquet original con todas las columnas
df = pd.read_parquet('estaciones_tasas_secado.parquet')

# Seleccionar solo las columnas "id" y "id_celda"
df_new = df[['id', 'id_celda']]

# Renombrar las columnas
df_new.columns = ['id_estacion', 'id_celda']

# Guardar el nuevo archivo Parquet
df_new.to_parquet('estaciones_tasas_secado.parquet', index=False)
