import dagster as dg
import geopandas as gpd

from cupum.partitions import state_partitions


@dg.asset(
    name="2010",
    key_prefix="houses",
    partitions_def=state_partitions,
    io_manager_key="gpkg_manager",
    group_name="houses",
)
def houses_2010() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(zip([], [], []), geometry=[], crs="ESRI:54009", columns=["TIPOMZA", "POBTOT", "CVEGEO"])