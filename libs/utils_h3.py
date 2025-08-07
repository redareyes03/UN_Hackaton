import geopandas as gpd
from shapely.geometry import Point
import h3  # h3-py v4.x

def point_to_h3(lat: float, lon: float, res: int = 5) -> str:
    """
    Convierte un punto (lat, lon) a un índice H3 de resolución `res`.
    """
    return h3.latlng_to_cell(lat, lon, res)

def geom_to_h3(geom_gdf: gpd.GeoDataFrame, res: int = 5) -> set:
    """
    Convierte una GeoDataFrame (puntos o polígonos) a un conjunto de índices H3.
    - Asegura que el CRS sea EPSG:4326 antes de procesar.
    - Para polígonos usa h3.geo_to_cells y, si falla, retorna solo el índice del centroide.
    """
    # Asegura CRS correcto
    if geom_gdf.crs.to_string() != 'EPSG:4326':
        geom_gdf = geom_gdf.to_crs(epsg=4326)

    hexes = set()
    for geom in geom_gdf.geometry:
        if isinstance(geom, Point):
            # Punto: un solo hexágono
            hexes.add(h3.latlng_to_cell(geom.y, geom.x, res))
        else:
            # Polígono: intenta generar celdas
            gj = geom.__geo_interface__
            try:
                cells = set(h3.geo_to_cells(gj, res))
                if not cells:
                    # Si no se generan celdas válidas, forzar excepción
                    raise ValueError("No celdas H3 generadas")
            except Exception:
                # Fallback: toma el hexágono del centroide
                c = geom.centroid
                cells = {h3.latlng_to_cell(c.y, c.x, res)}
            hexes |= cells

    return hexes
