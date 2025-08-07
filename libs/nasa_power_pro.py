# libs/chirps_wind.py

import datetime
import requests
import geopandas as gpd
import numpy as np
import h3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from libs.utils_h3 import geom_to_h3

# Mapear ISO → sufijo de archivo en el repositorio
FILENAME_MAP = {
    "NL":   "NL",
    "JAL":  "Jal",
    "CDMX": "Cdmx",
    "VER":  "Ver",
    "OAX":  "Oax",
}

BASE_URL = "https://api.open-meteo.com/v1/forecast"


def ingesta_viento_a_offset(
    estado_codigo: str,
    estado_siglas: str,
    dias_offset: int,
    res: int = 5
) -> dict[str, tuple[float, float, float]]:
    """
    Viento pronosticado (offset) por H3-cell:
    retorna { hex: (W_med, W_max, W_min) } en m/s,
    usando Open-Meteo en lugar de NASA POWER.
    """
    # 1) Determinar sufijo y cargar GeoJSON
    abbr = estado_siglas.upper()
    suffix = FILENAME_MAP.get(abbr)
    if not suffix:
        raise ValueError(f"No hay mapeo de sufijo para '{abbr}'")
    url_geo = (
        f"https://raw.githubusercontent.com/"
        f"open-mexico/mexico-geojson/main/"
        f"{estado_codigo}-{suffix}.geojson"
    )
    estado_gdf = gpd.read_file(url_geo).to_crs(epsg=4326)

    # 2) Generar grilla H3
    hexes = sorted(geom_to_h3(estado_gdf, res=res))

    # 3) Calcular fecha objetivo en ISO (YYYY-MM-DD)
    hoy      = datetime.date.today()
    target   = hoy + datetime.timedelta(days=dias_offset)
    date_str = target.isoformat()

    # 4) Función de fetch por hexágono
    def fetch(hex_id: str):
        lat, lon = h3.cell_to_latlng(hex_id)
        params = {
            "latitude":      lat,
            "longitude":     lon,
            "daily":         "wind_speed_10m_mean,wind_speed_10m_max,wind_speed_10m_min",
            "start_date":    date_str,
            "end_date":      date_str,
            "timezone":      "UTC"
        }
        try:
            resp = requests.get(BASE_URL, params=params, timeout=10)
            resp.raise_for_status()
            daily = resp.json().get("daily", {})
            # Extraer los únicos valores de la lista
            w_med = daily.get("wind_speed_10m_mean", [np.nan])[0]
            w_max = daily.get("wind_speed_10m_max",  [np.nan])[0]
            w_min = daily.get("wind_speed_10m_min",  [np.nan])[0]
            return hex_id, (float(w_med/10), float(w_max/10), float(w_min/10))
        except Exception:
            # En caso de error o dato faltante
            return hex_id, (np.nan, np.nan, np.nan)

    # 5) Ejecutar en paralelo
    results: dict[str, tuple[float, float, float]] = {}
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(fetch, h) for h in hexes]
        for fut in as_completed(futures):
            h, vals = fut.result()
            results[h] = vals

    print(f"✅ Viento offset Open-Meteo ({date_str}): "
          f"{len(results)}/{len(hexes)} celdas obtenidas.")
    return results


if __name__ == "__main__":
    # Prueba local
    sample = ingesta_viento_a_offset("19", "NL", dias_offset=1, res=5)
    for i, (h, vals) in enumerate(sample.items()):
        print(h, vals)
        if i >= 4:
            break