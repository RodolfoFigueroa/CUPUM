import geopandas as gpd

import dagster as dg
from cupum.assets.common import (
    merge_census_and_geometries,
)
from cupum.assets.load.census import load_census_loc_2020
from cupum.assets.load.geometries import load_loc_geometries_2020

from cupum.partitions import state_partitions


@dg.graph_asset(
    name="2020",
    key_prefix="locs",
    partitions_def=state_partitions,
    group_name="locs",
)
def locs() -> gpd.GeoDataFrame:
    census = load_census_loc_2020()
    geometries = load_loc_geometries_2020()
    return merge_census_and_geometries(census, geometries)
