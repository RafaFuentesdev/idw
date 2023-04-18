import pandas as pd
import numpy as np
import math

#STATIONS_FILE = "estaciones_lluvia.parquet"
#STATIONS_IDW_FILE = "idw_lluvia.parquet"
#STATIONS_FILE = "estaciones_temperatura.parquet"
#STATIONS_IDW_FILE = "idw_temperatura.parquet"
STATIONS_FILE = "estaciones_tasas_secado.parquet"
STATIONS_IDW_FILE = "idw_tasas_secado.parquet"
NUM_REF_POINTS = 3
ALPHA = 3
NCOLS = 13901


class Station:
    def __init__(self, saih_id, map_id, coord_x, coord_y):
        self.saih_id = saih_id
        self.map_id = map_id
        self.coord_x = coord_x
        self.coord_y = coord_y


class IDWInterpolator:
    def __init__(self, stations_file, num_ref_points, alpha, ncols):
        self.num_ref_points = num_ref_points
        self.alpha = alpha
        self.ncols = ncols
        self.stations = self.read_stations(stations_file)

    @staticmethod
    def read_stations(stations_file):
        stations_df = pd.read_parquet(stations_file)
        stations = []
        for _, row in stations_df.iterrows():
            stations.append(Station(row['id_estacion'], row['id_celda'], row['id_celda'] % NCOLS, row['id_celda'] // NCOLS))
        return stations

    def is_station(self, id):
        return any(station.map_id == id for station in self.stations)

    def compute_reference_points(self, id):
        x = id % self.ncols
        y = id // self.ncols
        coords = np.array([(station.coord_x, station.coord_y) for station in self.stations])
        dist = np.array([self.compute_distance(x, y, station) for station in self.stations])
        ref_points_indices = np.argpartition(dist, self.num_ref_points)[:self.num_ref_points]
        ref_points = [self.stations[i].saih_id for i in ref_points_indices]
        weights = 1 / np.power(dist[ref_points_indices], self.alpha)
        weights /= weights.sum()
        weights = self.adjust_decimals(weights)
        return np.array(ref_points), weights

    def compute_distance(self, x, y, station):
        return np.sqrt((x - station.coord_x) ** 2 + (y - station.coord_y) ** 2)

    @staticmethod
    def adjust_decimals(weights):
        rounded_weights = [round(weight, 3) for weight in weights]
        sum_weights = sum(rounded_weights)
        if not math.isclose(sum_weights, 1, rel_tol=1e-9):
            rounded_weights[-1] += round(1 - sum_weights, 3)
        return rounded_weights


def main():
    interpolator = IDWInterpolator(STATIONS_FILE, NUM_REF_POINTS, ALPHA, NCOLS)
    results = []

    # Load the propiedades_hidricas.parquet into a DataFrame
    propiedades_hidricas_df = pd.read_parquet("propiedades_hidricas.parquet")

    # Extract the ids from the DataFrame
    ids_to_process = propiedades_hidricas_df.index

    for id in ids_to_process:
        if not interpolator.is_station(id):
            ref_points, weights = interpolator.compute_reference_points(id)
            est_values = list(ref_points.astype(int))
            weight_values = list(weights)
            est_weights = [x for pair in zip(est_values, weight_values) for x in pair]
            results.append([id] + est_weights)
        else:
            print(f"Station found: {saih_id} - {id}, 1.0, 0.0, 0.0")
            station = next(station for station in interpolator.stations if station.map_id == id)
            saih_id = int(station.saih_id)
            est_1 = saih_id
            est_2 = saih_id
            est_3 = saih_id
            peso_1 = 1.0
            peso_2 = 0.0
            peso_3 = 0.0
            results.append([id, est_1, peso_1, est_2, peso_2, est_3, peso_3])
            # Print a message every 500,000 lines
            if len(results) % 500_000 == 0:
                print(f"Processed {len(results)} lines")

    output_df = pd.DataFrame(results, columns=["id", "est_1", "peso_1", "est_2", "peso_2", "est_3", "peso_3"])
    output_df.to_parquet(STATIONS_IDW_FILE, index=False)

if __name__ == "__main__":
    main()


