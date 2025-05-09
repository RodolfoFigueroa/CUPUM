from typing import Literal

import geopandas as gpd
import pandas as pd

import dagster as dg
from cupum.partitions import year_and_zone_partitions


def geometries_factory(name: Literal["GHSL", "WORLDPOP"]) -> dg.AssetsDefinition:
    @dg.asset(
        name="geometries",
        key_prefix=name,
        partitions_def=year_and_zone_partitions,
        ins={
            "blocks": dg.AssetIn([name, "blocks"]),
            "locs": dg.AssetIn([name, "locs"]),
        },
        io_manager_key="gpkg_manager",
        group_name=f"{name}",
    )
    def _asset(blocks: gpd.GeoDataFrame, locs: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        return gpd.GeoDataFrame(
            pd.concat(
                [
                    blocks[["CVEGEO", "POBTOT", "geometry"]],
                    locs[["CVEGEO", "POBTOT", "geometry"]],
                ]
            )
        )

    return _asset


dassets = [geometries_factory("GHSL"), geometries_factory("WORLDPOP")]
