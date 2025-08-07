# libs/flood_depression.py

import datetime
import requests
import geopandas as gpd
import numpy as np
import h3.api.basic_str as h3        # API de cadenas con grid_disk y cell_to_latlng
from libs.utils_h3 import geom_to_h3

# API pública de elevación
ELEVATION_API   = "https://api.open-elevation.com/api/v1/lookup"
# Umbral de desnivel (m) para riesgo máximo
DROP_THRESHOLD  = 100.0

def ingesta_inundaciones_por_estado(
    estado_codigo: str,
    estado_siglas: str,
    res: int = 5
) -> dict[str, float]:
    """
    Estima riesgo de inundación por depresión local:
      - Para cada hexágono:
          1) Elevación en el centro y de sus vecinos (k=1)
          2) diff = avg(elev_vecinos) - elev_centro
          3) riesgo = clip(diff / DROP_THRESHOLD, 0, 1)
      Devuelve { hex: riesgo_norm } en [0.0–1.0].
    """
    today = datetime.date.today().isoformat()

    # 1) Cargar polígono del estado y generar grilla H3
    suf     = estado_siglas if estado_siglas=="NL" else estado_siglas.capitalize()
    url_geo = f"https://raw.githubusercontent.com/open-mexico/mexico-geojson/main/{estado_codigo}-{suf}.geojson"
    estado_gdf = gpd.read_file(url_geo).to_crs(epsg=4326)
    hexes = sorted(geom_to_h3(estado_gdf, res=res))

    resultados: dict[str, float] = {}

    # 2) Procesar en batches para no saturar la API de elevación
    batch_size = 50
    for i in range(0, len(hexes), batch_size):
        batch = hexes[i:i+batch_size]

        # 2a) Preparar lista única de puntos: centro + vecinos
        pts, idx_map = [], {}
        for h in batch:
            # centro
            if h not in idx_map:
                lat, lon = h3.cell_to_latlng(h)
                idx_map[h] = len(pts); pts.append({"latitude": lat, "longitude": lon})
            # vecinos a distancia ≤1
            for n in h3.grid_disk(h, 1):
                if n not in idx_map:
                    lat, lon = h3.cell_to_latlng(n)
                    idx_map[n] = len(pts); pts.append({"latitude": lat, "longitude": lon})

        # 2b) Llamar a Open-Elevation
        resp = requests.post(ELEVATION_API, json={"locations": pts}, timeout=10)
        resp.raise_for_status()
        elevs = [r["elevation"] for r in resp.json()["results"]]

        # 3) Calcular riesgo para cada hex en el batch
        for h in batch:
            z0 = elevs[idx_map[h]]
            # obtener vecinos sin incluir el centro
            neigh = [n for n in h3.grid_disk(h, 1) if n != h]
            if not neigh:
                resultados[h] = 0.0
                continue
            z_neigh = [elevs[idx_map[n]] for n in neigh]
            diff = np.mean(z_neigh) - z0
            # normalizar y recortar
            risk = float(np.clip(diff / DROP_THRESHOLD, 0.0, 1.0))
            resultados[h] = risk

    print(f"✅ Flood-depression ({today}): {len(resultados)}/{len(hexes)} hexes procesados.")
    return resultados


if __name__ == "__main__":
    sample = ingesta_inundaciones_por_estado("19", "NL", res=5)
    for h, r in list(sample.items())[:5]:
        print(f"{h} → riesgo inundación: {r:.3f}")