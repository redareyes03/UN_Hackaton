import requests
import geopandas as gpd
import datetime
import h3
import streamlit as st
from libs.utils_h3 import geom_to_h3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def _normalize_sigla(sigla: str) -> str:
    return sigla.upper() if len(sigla) == 2 else sigla.capitalize()

@st.cache_data
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
            # Configure retry strategy
            retry_strategy = Retry(
                total=5,                # total retries
                backoff_factor=5,       # wait 5s, then 10s, then 20s...
                status_forcelist=[429, 500, 502, 503, 504],  # retry on these status codes
                allowed_methods=["GET","POST"],  # retry GET and POST requests
            )

            adapter = HTTPAdapter(max_retries=retry_strategy)

            # Create a session and mount adapter
            session = requests.Session()
            session.mount("https://", adapter)
            session.mount("http://", adapter)

            resp = session.get(
                "https://api.open-meteo.com/v1/forecast",
                params=params,
                timeout=(60, 360),  # connect timeout 60s, read timeout 360s
            )


            # resp = requests.get("https://api.open-meteo.com/v1/forecast", params=params, timeout=(60, 360))
            resp.raise_for_status()
            data = resp.json()
            prec = float(data["daily"]["precipitation_sum"][0])
        except Exception as e:
            print(f"Error precipitación H3 {h}: {e}")
        resultados[h] = prec

    return resultados