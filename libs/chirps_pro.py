import requests
import geopandas as gpd
import datetime
import h3
from libs.utils_h3 import geom_to_h3

def _normalize_sigla(sigla: str) -> str:
    return sigla.upper() if len(sigla) == 2 else sigla.capitalize()

def ingesta_precipitacion_a_offset(
    estado_codigo: str,
    estado_siglas: str,
    dias_offset: int,
    res: int = 5
) -> dict[str, float]:
    sig = _normalize_sigla(estado_siglas)

    hoy    = datetime.date.today()
    target = hoy + datetime.timedelta(days=dias_offset)
    fecha_str = target.isoformat()

    url = (
        f"https://raw.githubusercontent.com/"
        f"open-mexico/mexico-geojson/main/"
        f"{estado_codigo}-{sig}.geojson"
    )
    estado = gpd.read_file(url).to_crs(epsg=4326)
    if estado.empty:
        raise ValueError(f"Estado {estado_codigo}-{sig} no encontrado en {url}")
    hexes = geom_to_h3(estado, res=res)

    resultados: dict[str, float] = {}
    for h in hexes:
        lat, lon = h3.cell_to_latlng(h)
        params = {
            "latitude": lat,
            "longitude": lon,
            "daily": "precipitation_sum",
            "start_date": fecha_str,
            "end_date":   fecha_str,
            "timezone":  "UTC"
        }
        prec = 0.0
        try:
            resp = requests.get("https://api.open-meteo.com/v1/forecast", params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            prec = float(data["daily"]["precipitation_sum"][0])
        except Exception as e:
            print(f"Error precipitación H3 {h}: {e}")
        resultados[h] = prec

    return resultados