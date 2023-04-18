import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Leer el archivo de texto en un DataFrame de pandas
df = pd.read_csv('estaciones.txt', delimiter='\t', skiprows=1, header=None, names=['id', 'id_celda', 'x', 'y'])

# Seleccionar solo las columnas de interés
df = df[['id', 'id_celda']]
# Eliminar los duplicados basados en la columna "id_estacion"
df = df.drop_duplicates(subset=['id'])
# Eliminar la última fila
df = df[:-1]

# Escribir la tabla en formato parquet
df.to_parquet('id_estaciones.parquet', index=False)
