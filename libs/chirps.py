# libs/chirps_simple.py

import datetime
import requests
import h3
import numpy as np
import geopandas as gpd
from libs.utils_h3 import geom_to_h3

def ingesta_precipitacion_por_estado(
    estado_codigo: str,
    estado_siglas: str,
    res: int = 5,
    fecha: datetime.date | None = None
) -> dict[str, float]:
    """
    Ingesta diaria de precipitación para un estado usando Open-Meteo.
    - No requiere NetCDF ni autenticación.
    - Devuelve un dict {h3_hex: precip_mm}.
    """
    # 1) Fecha válida
    hoy = datetime.date.today()
    if fecha is None or fecha >= hoy:
        fecha = hoy - datetime.timedelta(days=1)
    date_str = fecha.isoformat()

    # 2) Construir grilla H3 del estado
    suffix = estado_siglas if estado_siglas == "NL" else estado_siglas.capitalize()
    url_geo = (
        f"https://raw.githubusercontent.com/open-mexico/"
        f"mexico-geojson/main/{estado_codigo}-{suffix}.geojson"
    )
    gdf = gpd.read_file(url_geo).to_crs(epsg=4326)
    hexes = sorted(geom_to_h3(gdf, res=res))

    # 3) Consultar API para cada hex
    resultados: dict[str, float] = {}
    for hex_id in hexes:
        lat, lon = h3.cell_to_latlng(hex_id)
        resp = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude":       lat,
                "longitude":      lon,
                "daily":          "precipitation_sum",
                "start_date":     date_str,
                "end_date":       date_str,
                "timezone":       "UTC"
            },
            timeout=10
        )
        resp.raise_for_status()
        data = resp.json()
        try:
            v = data["daily"]["precipitation_sum"][0]
        except (KeyError, IndexError):
            continue
        if v is None or np.isnan(v) or v < 0:
            continue
        resultados[hex_id] = float(v)

    print(f"✅ Open-Meteo precipitación ({date_str}): "
          f"{len(resultados)}/{len(hexes)} celdas con datos.")
    return resultados


if __name__ == "__main__":
    # Prueba rápida
    import datetime
    d = datetime.date(2025, 7, 4)
    m = ingesta_precipitacion_por_estado("19", "NL", res=5, fecha=d)
    print("Celdas con datos:", len(m))