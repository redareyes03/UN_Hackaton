# app.py

from datetime import date, timedelta
import streamlit as st
import pandas as pd
import geopandas as gpd
import pydeck as pdk
import h3
from shapely.ops import unary_union
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─── 1) Import dinámico de funciones de ingesta ─────────────────────────
def load_funcs():
    from libs.nasa_power       import ingesta_temperatura_por_estado, ingesta_viento_por_estado
    from libs.nasa_power_pro   import ingesta_viento_a_offset
    from libs.chirps_pro       import ingesta_precipitacion_a_offset
    from libs.chirps           import ingesta_precipitacion_por_estado
    from libs.osm              import ingesta_infraestructura_por_estado
    from libs.flood_depression import ingesta_inundaciones_por_estado
    from libs.population       import ingesta_poblacion_por_estado
    from libs.lhasa            import ingesta_lhasa_por_estado

    return (
        ingesta_temperatura_por_estado,
        ingesta_viento_por_estado,
        ingesta_viento_a_offset,
        ingesta_precipitacion_a_offset,
        ingesta_precipitacion_por_estado,
        ingesta_infraestructura_por_estado,
        ingesta_inundaciones_por_estado,
        ingesta_poblacion_por_estado,
        ingesta_lhasa_por_estado,
    )

(
    ingesta_temp,
    ingesta_wind_hist,
    ingesta_wind_off,
    ingesta_precip_off,
    ingesta_precip_hist,
    ingesta_osm,
    ingesta_flood,
    ingesta_pop,
    ingesta_lhasa
) = load_funcs()

# ─── 2) Códigos de estado y etiquetas ──────────────────────────────────
STATE_CODES = {"NL":"19","JAL":"14","CDMX":"09","VER":"30","OAX":"20"}
STATE_NAMES = {
    "NL":"Nuevo León","JAL":"Jalisco","CDMX":"Ciudad de México",
    "VER":"Veracruz","OAX":"Oaxaca"
}
INDICATORS = {
    "T2M_MAX":        "Temperatura máxima (°C)",
    "T2M_MIN":        "Temperatura mínima (°C)",
    "precip_mm":      "Lluvia pronosticada (mm)",
    "precip_mm_hist": "Lluvia histórica (mm)",
    "W_MED":          "Viento medio (m/s)",
    "W_MAX":          "Viento máximo (m/s)",
    "W_MIN":          "Viento mínimo (m/s)",
    "W_MED_OFF":      "Viento medio pronosticado (m/s)",
    "W_MAX_OFF":      "Viento máximo pronosticado (m/s)",
    "W_MIN_OFF":      "Viento mínimo pronosticado (m/s)",
    "flood_risk_100y":"Riesgo de inundación (0–1)",
    "pop_total":      "Población total",
    "hospitals":      "Hospitales",
    "clinics":        "Clínicas",
    "schools":        "Escuelas",
    "bus_stops":      "Paradas de autobús",
    "substations":    "Subestaciones eléctricas",
    "landuse":        "Uso de suelo",
    "lhasa_norm":     "Riesgo deslaves (0–1)"
}
OPTIONS = list(INDICATORS.keys())

# ─── 3) Función de carga cacheada con ThreadPoolExecutor ───────────────
@st.cache_data
def load_data(selected_inds, code, abbr, fecha_hist, forecast_days, resolution):
    from libs.utils_h3 import geom_to_h3

    # Normalizar siglas con mayúscula inicial para GeoJSON
    sig = abbr if abbr == "NL" else abbr.capitalize()

    # 0) Cargar GeoJSON del estado
    url = f"https://raw.githubusercontent.com/open-mexico/mexico-geojson/main/{code}-{sig}.geojson"
    estado_gdf = gpd.read_file(url).to_crs(epsg=4326)

    # 1) Generar grilla H3
    hexes = sorted(geom_to_h3(estado_gdf, res=resolution))
    df = pd.DataFrame({"h3": hexes})

    mappings: dict[str, dict] = {}
    osm_keys = [k for k in ("hospitals","schools","clinics","bus_stops","substations","landuse")
                if k in selected_inds]

    # 2) Ingestas en paralelo
    tasks = {}
    with ThreadPoolExecutor(max_workers=6) as executor:
        if "precip_mm_hist" in selected_inds:
            tasks["precip_mm_hist"] = executor.submit(
                lambda: ingesta_precip_hist(code, sig, res=resolution, fecha=fecha_hist) or {}
            )
        if "precip_mm" in selected_inds:
            tasks["precip_mm"] = executor.submit(lambda: (
                raw := ingesta_precip_off(code, sig, dias_offset=forecast_days, res=resolution)
            ) and (
                raw if isinstance(raw, dict) else dict(zip(raw["hex"], raw["precip_mm"]))
            ))
        if any(k in selected_inds for k in ("T2M_MAX","T2M_MIN")):
            tasks["temp"] = executor.submit(
                lambda: ingesta_temp(code, sig, res=resolution, fecha=fecha_hist, max_workers=4)
            )
        if osm_keys:
            def fetch_osm():
                df_o = ingesta_osm(sig, res=resolution)
                return df_o if isinstance(df_o, pd.DataFrame) else pd.DataFrame(columns=["hex"]+osm_keys)
            tasks["osm"] = executor.submit(fetch_osm)
        if any(k in selected_inds for k in ("W_MED","W_MAX","W_MIN")):
            tasks["wind_hist"] = executor.submit(
                lambda: ingesta_wind_hist(code, sig, res=resolution, fecha=fecha_hist, max_workers=4)
            )
        if any(k in selected_inds for k in ("W_MED_OFF","W_MAX_OFF","W_MIN_OFF")):
            tasks["wind_off"] = executor.submit(
                lambda: ingesta_wind_off(code, sig, dias_offset=forecast_days, res=resolution) or {}
            )
        if "flood_risk_100y" in selected_inds:
            tasks["flood"] = executor.submit(
                lambda: ingesta_flood(code, sig, res=resolution) or {}
            )
        if "pop_total" in selected_inds:
            tasks["pop"] = executor.submit(
                lambda: ingesta_pop(sig, res=resolution) or []
            )
        if any(k in selected_inds for k in ("lhasa_norm")):
            tasks["lhasa"] = executor.submit(
                lambda: ingesta_lhasa(code, sig, res=resolution)
            )

        fut_to_name = {fut: name for name, fut in tasks.items()}
        for fut in as_completed(fut_to_name):
            name = fut_to_name[fut]
            result = fut.result()

            if name == "temp":
                df_t = result; hex_col = "hex" if "hex" in df_t.columns else "h3"
                mappings["T2M_MAX"] = dict(zip(df_t[hex_col], df_t["T2M_max"]))
                mappings["T2M_MIN"] = dict(zip(df_t[hex_col], df_t["T2M_min"]))
            elif name == "osm":
                df_o = result
                for k in osm_keys:
                    mappings[k] = dict(zip(df_o["hex"], df_o[k]))
            elif name == "wind_hist":
                df_w = result; hex_col = "hex" if "hex" in df_w.columns else "h3"
                mappings["W_MED"] = dict(zip(df_w[hex_col], df_w["W_med"]))
                mappings["W_MAX"] = dict(zip(df_w[hex_col], df_w["W_max"]))
                mappings["W_MIN"] = dict(zip(df_w[hex_col], df_w["W_min"]))
            elif name == "wind_off":
                raw = result
                mappings["W_MED_OFF"] = {h:v[0] for h,v in raw.items()}
                mappings["W_MAX_OFF"] = {h:v[1] for h,v in raw.items()}
                mappings["W_MIN_OFF"] = {h:v[2] for h,v in raw.items()}
            elif name == "precip_mm_hist":
                mappings["precip_mm_hist"] = result
            elif name == "precip_mm":
                mappings["precip_mm"] = result
            elif name == "flood":
                mappings["flood_risk_100y"] = result
            elif name == "pop":
                mappings["pop_total"] = dict(result)
            elif name == "lhasa":
                mappings["lhasa_norm"] = {h:v[1] for h,v in result.items()}

    # 3) Mapear valores e índices de display
    for ind in selected_inds:
        df[ind] = df["h3"].map(lambda h: mappings.get(ind, {}).get(h, 0.0))
        df[f"{ind}_disp"] = df[ind].apply(lambda v: f"{v:.2f}" if pd.notna(v) else "N/A")

    # 4) Agregar lat/lon, hex_id e infraestructura
    df[["lat","lon"]] = pd.DataFrame(df["h3"].map(h3.cell_to_latlng).tolist(), index=df.index)
    df["hex_id"] = [f"{sig}_{i+1:03d}" for i in range(len(df))]
    infra_keys = ["hospitals","clinics","schools","bus_stops","substations","landuse"]
    for k in infra_keys:
        if k in df.columns:
            df[k] = df[k].fillna(0).astype(int)

    return df

# ─── 4) Interfaz Streamlit ──────────────────────────────────────────────
st.set_page_config(layout="wide", page_title="Mapa de Indicadores H3")
st.title("Mapa de Indicadores H3")

# Sidebar: configuración y carga
with st.sidebar.form("config_form"):
    st.header("Configuración")
    state_abbr   = st.selectbox("Estado", list(STATE_CODES.keys()), format_func=lambda k: STATE_NAMES[k])
    resolution   = st.slider("Resolución H3", 0, 10, 5)
    fecha_default= (date.today() - timedelta(days=3))
    fecha_hist   = st.date_input(
        "Fecha histórica",
        value=fecha_default,
        min_value=date(2021,1,1),
        max_value=fecha_default
    )
    indicators   = st.multiselect("Indicadores", options=OPTIONS, default=OPTIONS,
                                  format_func=lambda k: INDICATORS[k])
    forecast_days= st.slider("Días de pronóstico", 0, 30, 0)
    submit       = st.form_submit_button("Cargar datos")

if submit:
    code      = STATE_CODES[state_abbr]
    df        = load_data(indicators, code, state_abbr, fecha_hist, forecast_days, resolution)
    st.session_state.df         = df
    st.session_state.indicators = indicators

# Leer datos cargados
df         = st.session_state.get("df", pd.DataFrame())
indicators = st.session_state.get("indicators", [])

# Generar 'polygon' si no existe (necesario para renderizar)
if not df.empty and "polygon" not in df.columns:
    df["polygon"] = df["h3"].map(lambda h: [[lon, lat] for lat, lon in h3.cell_to_boundary(h)])

# Selector para colorear sin recargar datos
if not df.empty:
    color_by = st.sidebar.selectbox(
        "Colorear por", options=indicators,
        format_func=lambda k: INDICATORS[k]
    )
else:
    color_by = None

# ─── 5) Renderizado del mapa y métricas ─────────────────────────────────
if not df.empty:
    numeric = [c for c in indicators if pd.api.types.is_numeric_dtype(df[c])]
    color_col = color_by if color_by in numeric else (numeric[0] if numeric else None)

    if color_col:
        vmin, vmax = df[color_col].min(), df[color_col].max()
        df["color"] = df[color_col].apply(
            lambda v: [int(65 + (v-vmin)/(vmax-vmin)*(220-65)),
                       int(105 - (v-vmin)/(vmax-vmin)*(105-20)),
                       int(225 - (v-vmin)/(vmax-vmin)*(225-60)), 180]
        )
    else:
        df["color"] = [[0,128,0,120]] * len(df)

    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat), crs="EPSG:4326")
    centroid = unary_union(gdf.to_crs(epsg=3857).geometry).centroid
    centroid = gpd.GeoSeries([centroid], crs="EPSG:3857").to_crs(epsg=4326).iloc[0]

    tooltip_html = (
        "<b>ID:</b> {hex_id}<br>"
        + "".join(f"<b>{INDICATORS[c]}:</b> {{{c}_disp}}<br>" for c in indicators)
    )

    st.pydeck_chart(
        pdk.Deck(
            layers=[
                pdk.Layer("TileLayer", None,
                          url="https://c.tile.openstreetmap.org/{z}/{x}/{y}.png",
                          tile_size=256),
                pdk.Layer("PolygonLayer", gdf,
                          get_polygon="polygon", pickable=True,
                          get_fill_color="color", stroked=True,
                          get_line_color=[0,0,0], line_width_min_pixels=1)
            ],
            initial_view_state=pdk.ViewState(
                latitude=centroid.y, longitude=centroid.x, zoom=6
            ),
            tooltip={"html": tooltip_html}
        ),
        use_container_width=True, height=700
    )

    st.subheader("Métricas por Hexágono")
    chosen = st.text_input("Ingresa ID (p.ej. NL_001):", "")
    if chosen:
        sel = df[df.hex_id == chosen]
        if sel.empty:
            st.warning("ID no encontrado.")
        else:
            group_defs = [
                ("Temperatura (°C)",      ["T2M_MAX", "T2M_MIN"]),
                ("Precipitación (mm)",     ["precip_mm", "precip_mm_hist"]),
                ("Viento histórico (m/s)", ["W_MED", "W_MAX", "W_MIN"]),
                ("Viento pronost. (m/s)",  ["W_MED_OFF", "W_MAX_OFF", "W_MIN_OFF"]),
                ("Inundación & Población", ["flood_risk_100y","pop_total"]),
                ("Infraestructura",        ["hospitals","clinics","schools","bus_stops","substations","landuse"]),
                ("Deslaves",               ["lhasa_norm"]),
            ]
            infra_keys = ["hospitals","clinics","schools","bus_stops","substations","landuse"]
            cols = st.columns(len(group_defs))
            for (title, keys), col in zip(group_defs, cols):
                present = [k for k in keys if k in indicators]
                if not present:
                    continue
                col.markdown(f"**{title}**")
                for k in present:
                    val  = sel.iloc[0][k]
                    disp = f"{val:.2f}" if pd.notna(val) else "N/A"
                    if k in infra_keys:
                        disp = f"{int(val)}"
                    col.metric(INDICATORS[k], disp)