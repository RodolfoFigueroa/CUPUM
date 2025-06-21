import geopandas as gpd

import dagster as dg
from cupum.assets.common import merge_census_and_geometries
from cupum.assets.load.census import (
    load_census_blocks_and_houses_2010,
)
from cupum.assets.load.geometries import load_block_geometries_2010
from cupum.partitions import state_partitions


@dg.graph_asset(
    name="base_2010",
    key_prefix="blocks",
    partitions_def=state_partitions,
    group_name="blocks",
)
def blocks_2010() -> gpd.GeoDataFrame:
    geometries = load_block_geometries_2010()
    census = load_census_blocks_and_houses_2010()
    return merge_census_and_geometries(census, geometries)


@dg.asset(
    name="cut_2010",
    key_prefix="blocks",
    ins={"census_blocks": dg.AssetIn(["blocks", "base_2010"])},
    partitions_def=state_partitions,
    io_manager_key="gpkg_manager",
    group_name="blocks",
)
def cut_2010(census_blocks: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return census_blocks.query("POBTOT > 0")