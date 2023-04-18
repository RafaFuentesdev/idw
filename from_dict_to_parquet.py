import pandas as pd
from stations import STATION_DICT

# Create the rainfall stations dataframe
rainfall_df = pd.DataFrame(columns=["id_estacion", "id_celda"])
for key, value in STATION_DICT.items():
    if value["rainfall"]:
        id_estacion = key.split(".")[0]
        id_celda = value["id"]
        rainfall_df = pd.concat([rainfall_df, pd.DataFrame({"id_estacion": [id_estacion], "id_celda": [id_celda]})])

rainfall_df = rainfall_df.reset_index(drop=True)

# Create the temperature stations dataframe
temperature_df = pd.DataFrame(columns=["id_estacion", "id_celda"])
for key, value in STATION_DICT.items():
    if value["temperature"]:
        id_estacion = key.split(".")[0]
        id_celda = value["id"]
        temperature_df = pd.concat([temperature_df, pd.DataFrame({"id_estacion": [id_estacion], "id_celda": [id_celda]})])

temperature_df = temperature_df.reset_index(drop=True)

# Write the dataframes to parquet files
rainfall_df.to_parquet("estaciones_lluvia.parquet")
temperature_df.to_parquet("estaciones_temperatura.parquet")
