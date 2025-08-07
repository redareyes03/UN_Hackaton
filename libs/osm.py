import datetime
import h3
import geopandas as gpd
import osmnx as ox
from pathlib import Path
from collections import Counter
from libs.utils_h3 import geom_to_h3

# Configurar OSMnx
ox.settings.use_cache = True
ox.settings.log_console = False
ox.settings.overpass_max_query_area_size = 1_000_000_000  # hasta 1 000 km²
ox.settings.overpass_max_query_area_factor = 1           # sin subdividir

# Mapear ISO → nombre de estado
STATE_NAME_MAP = {
    "NL":   "Nuevo León",
    "JAL":  "Jalisco",
    "CDMX": "Ciudad de México",
    "VER":  "Veracruz",
    "OAX":  "Oaxaca",
}

# Carpeta de caché
CACHE_DIR = Path("data/osm_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

def load_or_fetch(path: Path, fetch_fn):
    if path.exists():
        return gpd.read_file(path)
    gdf = fetch_fn()
    gdf.to_file(path, driver="GPKG")
    return gdf

def ingesta_infraestructura_por_estado(estado_siglas: str, res: int = 5) -> gpd.GeoDataFrame:
    code = estado_siglas.upper()
    state_name = STATE_NAME_MAP.get(code)
    if not state_name:
        raise ValueError(f"No hay mapeo para '{code}'")

    # 1) Polígono del estado (caché)
    state_path = CACHE_DIR / f"{code}_state.gpkg"
    state_gdf = load_or_fetch(
        state_path,
        lambda: ox.geocode_to_gdf(f"{state_name}, Mexico")
    ).to_crs(epsg=4326)
    poly = state_gdf.loc[0, "geometry"]

    # 2) Capas puntuales
    point_layers = {
        "hospitals":   {"amenity": ["hospital"]},
        "schools":     {"amenity": ["school"]},
        "clinics":     {"amenity": ["clinic"]},
        "bus_stops":   {"highway": ["bus_stop"]},
        "substations": {"power": ["substation"]},
        "landuse":     {"landuse": True},
    }

    # 3) Cargar cada capa (cache + fetch)
    data = {}
    for key, tags in point_layers.items():
        path = CACHE_DIR / f"{key}_{code}.gpkg"
        data[key] = load_or_fetch(
            path,
            lambda tags=tags: ox.features_from_polygon(poly, tags).to_crs(epsg=4326)
        )

    # 4) Generar grilla H3
    poly_gdf = gpd.GeoDataFrame(geometry=[poly], crs="EPSG:4326")
    hexes = geom_to_h3(poly_gdf, res=res)
    infra = {h: {k: 0 for k in point_layers} for h in hexes}

    # 5) Contar puntos por celda con Counter
    for key in point_layers:
        gdf = data[key]
        if gdf.empty:
            continue
        centroids = gdf.geometry.centroid
        cells = centroids.apply(lambda pt: h3.latlng_to_cell(pt.y, pt.x, res))
        for h, cnt in Counter(cells).items():
            if h in infra:
                infra[h][key] = cnt

    # 6) Devolver GeoDataFrame listo para el merge en load_data
    df_out = gpd.pd.DataFrame([
        {"hex": h, **infra[h]}
        for h in hexes
    ])
    return df_out

if __name__ == "__main__":
    df = ingesta_infraestructura_por_estado("OAX", res=5)
    print(df.head())
    print("Total celdas:", len(df))