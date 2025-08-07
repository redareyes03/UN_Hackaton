import datetime
from pathlib import Path

import geopandas as gpd
import rasterio
from rasterio.mask import mask        # <-- corregido: importamos la función mask
import osmnx as ox
import h3
from libs.utils_h3 import geom_to_h3

# Configuración OSMnx para geocodificación
ox.settings.use_cache = True
ox.settings.log_console = False
ox.settings.overpass_max_query_area_size = 1_000_000_000
ox.settings.overpass_max_query_area_factor = 1

# Ruta y URL del GeoTIFF de población de México 2025 (100 m)
POP_DIR = Path("data/population")
POP_TIF = POP_DIR / "mex_pop_2025_100m.tif"
POP_URL = (
    "https://data.worldpop.org/GIS/Population/Global_2015_2030/R2024B/"
    "2025/MEX/v1/100m/constrained/mex_pop_2025_CN_100m_R2024B_v1.tif"
)

def download_population():
    """Descarga el GeoTIFF de población si no existe."""
    if not POP_TIF.exists():
        POP_DIR.mkdir(parents=True, exist_ok=True)
        import requests
        r = requests.get(POP_URL, stream=True)
        r.raise_for_status()
        with open(POP_TIF, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)
        print(f"Descargado: {POP_TIF}")
    else:
        print(f"Usando existente: {POP_TIF}")

def ingesta_poblacion_por_estado(estado_siglas: str, res: int = 5) -> list[tuple[str,int]]:
    """
    Retorna lista de tuplas (hex, población) para celdas H3 de un estado.
    """
    download_population()
    with rasterio.open(POP_TIF) as src:
        nodata = src.nodata

        # 1) Polígono del estado
        state_map = {
            "NL":   "Nuevo León",
            "JAL":  "Jalisco",
            "CDMX": "Ciudad de México",
            "VER":  "Veracruz",
            "OAX":  "Oaxaca",
        }
        code = estado_siglas.upper()
        name = state_map.get(code)
        if not name:
            raise ValueError(f"No hay mapeo para '{code}'")
        gdf = ox.geocode_to_gdf(f"{name}, Mexico").to_crs(epsg=4326)
        poly = gdf.geometry.iloc[0]

        # 2) Recortar ráster correctamente
        out_image, out_transform = mask(src, [poly], crop=True)
        arr = out_image[0]

    # 3) Generar la misma grilla H3
    poly_gdf = gpd.GeoDataFrame(geometry=[poly], crs="EPSG:4326")
    hexes = geom_to_h3(poly_gdf, res=res)
    infra = {h: 0 for h in hexes}

    # 4) Acumular población por píxel
    rows, cols = arr.shape
    for i in range(rows):
        for j in range(cols):
            v = arr[i, j]
            if v == nodata or v <= 0:
                continue
            lon, lat = out_transform * (j + 0.5, i + 0.5)
            h = h3.latlng_to_cell(lat, lon, res)
            if h in infra:
                infra[h] += int(v)

    # 5) Imprimir resumen y devolver lista
    today = datetime.date.today().isoformat()
    for cell in hexes:
        pop = infra[cell]
        print(f"{today} | {code} | H3: {cell} | pop_total: {pop}")
    print(f"Ingesta población para {code} completada ({len(hexes)} celdas).")

    return list(infra.items())

if __name__ == "__main__":
    data = ingesta_poblacion_por_estado("NL", res=5)
    print("Total celdas:", len(data))
    print(data[:5])