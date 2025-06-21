import geopandas as gpd

import dagster as dg

from cupum.assets.common import merge_census_and_geometries
from cupum.assets.load.geometries import load_ageb_geometries_2010
from cupum.assets.load.census import load_census_agebs_2010
from cupum.partitions import state_partitions


@dg.graph_asset(
    name="2010",
    key_prefix="agebs",
    partitions_def=state_partitions,
    group_name="agebs",
)
def census_agebs_2010() -> gpd.GeoDataFrame:
    census = load_census_agebs_2010()
    geometries = load_ageb_geometries_2010()
    return merge_census_and_geometries(census, geometries)
