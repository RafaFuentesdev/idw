import pandas as pd

# Leer el archivo Excel sin la primera fila de encabezado y la primera columna
df_excel = pd.read_excel('ParmetrosSueloPepe.xlsx', header=1, usecols=[1,2,3,4,5,6])


print(df_excel.columns)
print(df_excel.head())

# Crear un nuevo DataFrame con las columnas necesarias
df_parquet = pd.DataFrame({
    'id': df_excel['Cod'],
    'cap_total': df_excel['Captotal'],
    'ts_hasta_cc': df_excel['TS hasta CC'],
    'ts_tras_cc': df_excel['TS tras CC'],
    'ts_tras_pm': df_excel['TS tras PM']
})

# Escribir el nuevo DataFrame en un archivo Parquet
df_parquet.to_parquet('tasas_secado.parquet', index=False)