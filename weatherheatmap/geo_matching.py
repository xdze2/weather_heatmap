import geopandas as gpd
from shapely.geometry import Point

# Load administrative boundaries (departments and cantons)
departments = gpd.read_file("data/assets/departements-version-simplifiee.geojson")
cantons = gpd.read_file(
    "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/cantons.geojson"
)


def get_departement(lat: float, lon: float) -> str:
    """Return the department and canton for a given GPS coordinate (lat, lon)."""
    point = Point(lon, lat)

    dept_match = departments[departments.contains(point)]
    department = dept_match.iloc[0]["code"] if not dept_match.empty else None

    return department


def get_canton(lat: float, lon: float) -> str:
    """Return the department and canton for a given GPS coordinate (lat, lon)."""
    point = Point(lon, lat)

    canton_match = cantons[cantons.contains(point)]
    canton = canton_match.iloc[0]["code"] if not canton_match.empty else None

    return canton
