# libs/landslide_risk.py

import datetime
import requests
import geopandas as gpd
import numpy as np
import h3.api.basic_str as h3               # API de cadenas con grid_disk y cell_to_latlng :contentReference[oaicite:4]{index=4}
from libs.utils_h3 import geom_to_h3

# API pública de elevación de Open-Elevation
ELEVATION_API = "https://api.open-elevation.com/api/v1/lookup"

def ingesta_lhasa_por_estado(
    estado_codigo: str,
    estado_siglas: str,
    res: int = 5
) -> dict[str, tuple[float, float]]:
    """
    Riesgo de deslaves por pendiente local:
      devuelve { hex: (max_slope_deg, risk_norm) } donde
        max_slope_deg = máxima pendiente en grados
        risk_norm      = max_slope_deg / 60, recortado a [0–1]
    """
    # 1) Obtener grilla H3 del estado
    suf     = estado_siglas.upper() if estado_siglas=="NL" else estado_siglas.capitalize()
    url_geo = (
        f"https://raw.githubusercontent.com/open-mexico/"
        f"mexico-geojson/main/{estado_codigo}-{suf}.geojson"
    )
    gdf   = gpd.read_file(url_geo).to_crs(epsg=4326)
    hexes = sorted(geom_to_h3(gdf, res=res))

    results = {}
    today   = datetime.date.today().isoformat()

    # 2) Procesar en batches para no saturar la API de elevación
    batch_size = 50
    for i in range(0, len(hexes), batch_size):
        batch = hexes[i:i+batch_size]

        # 2a) Preparar lista de puntos: centro + vecinos (grid_disk k=1) :contentReference[oaicite:5]{index=5}
        pts, idx_map = [], {}
        for h in batch:
            if h not in idx_map:
                lat, lon = h3.cell_to_latlng(h)    # centro :contentReference[oaicite:6]{index=6}
                idx_map[h] = len(pts); pts.append({"latitude": lat, "longitude": lon})
            for n in h3.grid_disk(h, 1):         # incluye centro + vecinos :contentReference[oaicite:7]{index=7}
                if n not in idx_map:
                    lat, lon = h3.cell_to_latlng(n)
                    idx_map[n] = len(pts); pts.append({"latitude": lat, "longitude": lon})

        # 2b) Llamada batch a Open-Elevation para obtener elevaciones :contentReference[oaicite:8]{index=8}
        resp = requests.post(ELEVATION_API, json={"locations": pts}, timeout=10)
        resp.raise_for_status()
        elevs = [r["elevation"] for r in resp.json()["results"]]

        # 3) Cálculo de la pendiente máxima y normalización
        for h in batch:
            z0 = elevs[idx_map[h]]
            max_slope = 0.0
            lat0, lon0 = h3.cell_to_latlng(h)
            for n in h3.grid_disk(h, 1):
                if n == h:
                    continue
                z1 = elevs[idx_map[n]]
                # distancia haversine
                φ1, φ2 = np.radians(lat0), np.radians(h3.cell_to_latlng(n)[0])
                Δφ      = φ2 - φ1
                Δλ      = np.radians(h3.cell_to_latlng(n)[1] - lon0)
                a       = np.sin(Δφ/2)**2 + np.cos(φ1)*np.cos(φ2)*np.sin(Δλ/2)**2
                d       = 6371000 * 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
                if d > 0:
                    slope_deg = np.degrees(np.arctan(abs(z1 - z0) / d))
                    max_slope = max(max_slope, slope_deg)
            # normalizar [0–60°] → [0–1]
            results[h] = (round(max_slope, 2), round(min(max_slope/60.0, 1.0), 3))

    print(
        f"✅ Riesgo deslaves para {estado_siglas} ({today}): "
        f"{len(results)}/{len(hexes)} hexes procesados."
    )
    return results


if __name__ == "__main__":
    # Prueba rápida
    muestra = ingesta_lhasa_por_estado("19", "NL", res=5)
    for h, (pend, riesgo) in list(muestra.items())[:5]:
        print(f"{h} → pendiente: {pend}°, riesgo: {riesgo}")