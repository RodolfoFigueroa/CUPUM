
import geopandas as gpd
import pandas as pd

import dagster as dg
from cupum.assets.common import (
    load_block_geometries,
    load_census_blocks_and_houses,
    merge_census_and_geometries,
)
from cupum.partitions import state_and_year_partitions


@dg.graph_asset(
    name="base",
    key_prefix="blocks",
    partitions_def=state_and_year_partitions,
    group_name="blocks",
)
def census_blocks() -> gpd.GeoDataFrame:
    census = load_census_blocks_and_houses()
    geometries = load_block_geometries()
    return merge_census_and_geometries(census, geometries)


@dg.asset(
    name="cut",
    key_prefix="blocks",
    ins={"census_blocks": dg.AssetIn(["blocks", "base"])},
    partitions_def=state_and_year_partitions,
    io_manager_key="gpkg_manager",
    group_name="blocks",
)
def census_blocks_cut(census_blocks: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    df_container = census_blocks.query("TIPOMZA == 'Contenedora'")
    df_contained = census_blocks.query("TIPOMZA != 'Contenedora'")

    try:
        df_container_cut = df_container.overlay(df_contained, how="difference")
    except IndexError:
        df_container_cut = df_container

    if len(df_container) != len(df_container_cut):
        err = "Container blocks have been cut, but the number of rows is not equal"
        raise ValueError(err)

    return gpd.GeoDataFrame(
        pd.concat([df_container_cut, df_contained], ignore_index=True).query(
            "POBTOT != 0"
        )
    )
