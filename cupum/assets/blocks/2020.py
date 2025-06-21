import geopandas as gpd
import pandas as pd

import dagster as dg
from cupum.assets.common import (
    merge_census_and_geometries,
)
from cupum.assets.load.census import load_census_blocks_and_houses_2020
from cupum.assets.load.geometries import load_block_geometries_2020
from cupum.partitions import state_partitions


@dg.graph_asset(
    name="base_2020",
    key_prefix="blocks",
    partitions_def=state_partitions,
    group_name="blocks",
)
def census_blocks() -> gpd.GeoDataFrame:
    census = load_census_blocks_and_houses_2020()
    geometries = load_block_geometries_2020()
    return merge_census_and_geometries(census, geometries)


@dg.asset(
    name="cut_2020",
    key_prefix="blocks",
    ins={"census_blocks": dg.AssetIn(["blocks", "base_2020"])},
    partitions_def=state_partitions,
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
