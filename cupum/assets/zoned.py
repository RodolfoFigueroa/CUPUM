from typing import Literal

import geopandas as gpd
import pandas as pd

import dagster as dg
from cupum.assets.common import intersect_mesh_with_geoms
from cupum.partitions import year_and_zone_partitions


def zone_blocks_factory(name: Literal["GHSL", "WORLDPOP"]) -> dg.AssetsDefinition:
    @dg.asset(
        name="blocks",
        key_prefix=name,
        ins={
            "census_blocks": dg.AssetIn(
                ["blocks", "cut"], partition_mapping=dg.AllPartitionMapping()
            ),
            "mesh": dg.AssetIn([name, "mesh", "pop_coarse"]),
        },
        partitions_def=year_and_zone_partitions,
        io_manager_key="gpkg_manager",
        group_name=name,
    )
    def _asset(
        mesh: gpd.GeoDataFrame, census_blocks: dict[str, gpd.GeoDataFrame]
    ) -> gpd.GeoDataFrame:
        return intersect_mesh_with_geoms(mesh, census_blocks, predicate="intersects")

    return _asset


def zone_locs_factory(name: Literal["GHSL", "WORLDPOP"]) -> dg.AssetsDefinition:
    @dg.asset(
        name="locs",
        key_prefix=name,
        ins={
            "census_locs": dg.AssetIn(
                ["locs", "base"], partition_mapping=dg.AllPartitionMapping()
            ),
            "mesh": dg.AssetIn([name, "mesh", "pop_coarse"]),
        },
        partitions_def=year_and_zone_partitions,
        io_manager_key="gpkg_manager",
        group_name=f"{name}",
    )
    def _asset(
        census_locs: dict[str, gpd.GeoDataFrame], mesh: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        return intersect_mesh_with_geoms(
            mesh, census_locs, predicate="intersects"
        ).query("AMBITO == 'Rural'")

    return _asset


def zone_houses_factory(name: Literal["GHSL", "WORLDPOP"]) -> dg.AssetsDefinition:
    @dg.asset(
        name="houses",
        key_prefix=name,
        ins={
            "census_blocks": dg.AssetIn(
                ["houses", "base"], partition_mapping=dg.AllPartitionMapping()
            ),
            "mesh": dg.AssetIn([name, "mesh", "pop_coarse"]),
        },
        partitions_def=year_and_zone_partitions,
        io_manager_key="gpkg_manager",
        group_name=name,
    )
    def _asset(
        mesh: gpd.GeoDataFrame, census_blocks: dict[str, gpd.GeoDataFrame]
    ) -> gpd.GeoDataFrame:
        return intersect_mesh_with_geoms(mesh, census_blocks, predicate="within")

    return _asset


dassets = [
    zone_blocks_factory("GHSL"),
    zone_blocks_factory("WORLDPOP"),
    zone_locs_factory("GHSL"),
    zone_locs_factory("WORLDPOP"),
    zone_houses_factory("GHSL"),
    zone_houses_factory("WORLDPOP"),
]
