from typing import Literal

import geopandas as gpd

import dagster as dg
from cupum.assets.common import intersect_mesh_with_geoms
from cupum.partitions import year_and_zone_partitions


def zone_agebs_factory(name: Literal["GHSL", "WORLDPOP"]) -> dg.AssetsDefinition:
    name_ext = f"{name}_fine"

    @dg.asset(
        name="agebs",
        key_prefix=name_ext,
        ins={
            "census_agebs": dg.AssetIn(
                ["agebs", "base"], partition_mapping=dg.MultiPartitionMapping(
                    {
                        "year": dg.DimensionPartitionMapping(
                            dimension_name="year",
                            partition_mapping=dg.IdentityPartitionMapping(),
                        )
                    }
                ),
            ),
            "mesh": dg.AssetIn([name_ext, "mesh", "pop"]),
        },
        partitions_def=year_and_zone_partitions,
        io_manager_key="gpkg_manager",
        group_name=name_ext,
        tags={"concurrency": "limited"},
    )
    def _asset(
        mesh: gpd.GeoDataFrame, census_agebs: dict[str, gpd.GeoDataFrame]
    ) -> gpd.GeoDataFrame:
        return (
            intersect_mesh_with_geoms(mesh, census_agebs, predicate="intersects")
            .reset_index(drop=True)
            .reset_index(names="ageb_idx")
        )

    return _asset


def zone_blocks_factory(
    name: Literal["GHSL", "LANDSCAN", "WORLDPOP"],
) -> dg.AssetsDefinition:
    name_ext = f"{name}_coarse"

    @dg.asset(
        name="blocks",
        key_prefix=name_ext,
        ins={
            "census_blocks": dg.AssetIn(
                ["blocks", "base"],
                partition_mapping=dg.MultiPartitionMapping(
                    {
                        "year": dg.DimensionPartitionMapping(
                            dimension_name="year",
                            partition_mapping=dg.IdentityPartitionMapping(),
                        )
                    }
                ),
            ),
            "mesh": dg.AssetIn([name_ext, "mesh", "pop"]),
        },
        partitions_def=year_and_zone_partitions,
        io_manager_key="gpkg_manager",
        group_name=name_ext,
    )
    def _asset(
        mesh: gpd.GeoDataFrame, census_blocks: dict[str, gpd.GeoDataFrame]
    ) -> gpd.GeoDataFrame:
        return intersect_mesh_with_geoms(mesh, census_blocks, predicate="intersects")

    return _asset


def zone_locs_factory(
    name: Literal["GHSL", "LANDSCAN", "WORLDPOP"],
) -> dg.AssetsDefinition:
    name_ext = f"{name}_coarse"

    @dg.asset(
        name="locs",
        key_prefix=name_ext,
        ins={
            "census_locs": dg.AssetIn(
                ["locs", "base"], partition_mapping=dg.AllPartitionMapping()
            ),
            "mesh": dg.AssetIn([name_ext, "mesh", "pop"]),
        },
        partitions_def=year_and_zone_partitions,
        io_manager_key="gpkg_manager",
        group_name=name_ext,
    )
    def _asset(
        census_locs: dict[str, gpd.GeoDataFrame], mesh: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        return intersect_mesh_with_geoms(
            mesh, census_locs, predicate="intersects"
        ).query("AMBITO == 'Rural'")

    return _asset


def zone_houses_factory(
    name: Literal["GHSL", "LANDSCAN", "WORLDPOP"],
) -> dg.AssetsDefinition:
    name_ext = f"{name}_coarse"

    @dg.asset(
        name="houses",
        key_prefix=name_ext,
        ins={
            "census_blocks": dg.AssetIn(
                ["houses", "base"], partition_mapping=dg.AllPartitionMapping()
            ),
            "mesh": dg.AssetIn([name_ext, "mesh", "pop"]),
        },
        partitions_def=year_and_zone_partitions,
        io_manager_key="gpkg_manager",
        group_name=name_ext,
    )
    def _asset(
        mesh: gpd.GeoDataFrame, census_blocks: dict[str, gpd.GeoDataFrame]
    ) -> gpd.GeoDataFrame:
        return intersect_mesh_with_geoms(mesh, census_blocks, predicate="within")

    return _asset


dassets = [
    zone_blocks_factory("GHSL"),
    zone_blocks_factory("LANDSCAN"),
    zone_blocks_factory("WORLDPOP"),
    zone_locs_factory("GHSL"),
    zone_locs_factory("LANDSCAN"),
    zone_locs_factory("WORLDPOP"),
    zone_houses_factory("GHSL"),
    zone_houses_factory("LANDSCAN"),
    zone_houses_factory("WORLDPOP"),
    zone_agebs_factory("GHSL"),
    zone_agebs_factory("WORLDPOP"),
]
