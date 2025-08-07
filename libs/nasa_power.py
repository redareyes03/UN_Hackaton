import requests
import datetime
import pandas as pd
import geopandas as gpd
import h3
from concurrent.futures import ThreadPoolExecutor, as_completed
from libs.utils_h3 import geom_to_h3

NASA_POWER_API_URL = 'https://power.larc.nasa.gov/api/temporal/daily/point'
DEFAULT_PARAMS = {'community': 'AG', 'format': 'JSON'}

def get_power_data(lat: float, lon: float, date_str: str, params_list: list[str]) -> dict:
    parameters = ','.join(params_list)
    params = {
        **DEFAULT_PARAMS,
        'latitude': lat,
        'longitude': lon,
        'start': date_str,
        'end': date_str,
        'parameters': parameters
    }
    resp = requests.get(NASA_POWER_API_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()['properties']['parameter']

def _normalize_sigla(sigla: str) -> str:
    # Si tiene 2 letras, manténlas siempre en mayúscula (p.ej. NL)
    return sigla.upper() if len(sigla) == 2 else sigla.capitalize()

def _load_estado_gdf(estado_codigo: str, estado_siglas: str) -> gpd.GeoDataFrame:
    sig = _normalize_sigla(estado_siglas)
    url = f"https://raw.githubusercontent.com/open-mexico/mexico-geojson/main/{estado_codigo}-{sig}.geojson"
    gdf = gpd.read_file(url).to_crs(epsg=4326)
    if gdf.empty:
        raise ValueError(f"Estado {estado_codigo}-{sig} no encontrado en {url}")
    return gdf

def ingesta_temperatura_por_estado(
    estado_codigo: str,
    estado_siglas: str,
    res: int = 5,
    fecha: datetime.date | None = None,
    max_workers: int = 10
) -> pd.DataFrame:
    if fecha is None:
        fecha = datetime.date.today() - datetime.timedelta(days=1)
    date_str = fecha.strftime('%Y%m%d')

    estado_gdf = _load_estado_gdf(estado_codigo, estado_siglas)
    hexes = geom_to_h3(estado_gdf, res=res)

    def worker(h: str) -> dict:
        lat, lon = h3.cell_to_latlng(h)
        data = get_power_data(lat, lon, date_str, ['T2M','T2M_MAX','T2M_MIN'])
        return {
            'hex':    h,
            'T2M_med': data['T2M'][date_str],
            'T2M_max': data['T2M_MAX'][date_str],
            'T2M_min': data['T2M_MIN'][date_str]
        }

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futures = [exe.submit(worker, h) for h in hexes]
        for fut in as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as e:
                print(f"Error temperatura H3: {e}")

    return pd.DataFrame(results)

def ingesta_viento_por_estado(
    estado_codigo: str,
    estado_siglas: str,
    res: int = 5,
    fecha: datetime.date | None = None,
    max_workers: int = 10
) -> pd.DataFrame:
    if fecha is None:
        fecha = datetime.date.today() - datetime.timedelta(days=1)
    date_str = fecha.strftime('%Y%m%d')

    estado_gdf = _load_estado_gdf(estado_codigo, estado_siglas)
    hexes = geom_to_h3(estado_gdf, res=res)

    def worker(h: str) -> dict:
        lat, lon = h3.cell_to_latlng(h)
        data = get_power_data(lat, lon, date_str, ['WS10M','WS10M_MAX','WS10M_MIN'])
        return {
            'hex':  h,
            'W_med': data['WS10M'][date_str],
            'W_max': data['WS10M_MAX'][date_str],
            'W_min': data['WS10M_MIN'][date_str]
        }

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as exe:
        futures = [exe.submit(worker, h) for h in hexes]
        for fut in as_completed(futures):
            try:
                results.append(fut.result())
            except Exception as e:
                print(f"Error viento H3: {e}")

    return pd.DataFrame(results)