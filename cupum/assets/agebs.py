import geopandas as gpd

import dagster as dg
from cupum.assets.common import (
    load_ageb_geometries,
    load_census_agebs,
    merge_census_and_geometries,
)
from cupum.assets.common import intersect_mesh_with_geoms
from cupum.partitions import state_and_year_partitions, year_and_zone_partitions
from typing import Literal


@dg.graph_asset(
    name="base",
    key_prefix="agebs",
    partitions_def=state_and_year_partitions,
    group_name="agebs",
)
def census_agebs() -> gpd.GeoDataFrame:
    census = load_census_agebs()
    geometries = load_ageb_geometries()
    return merge_census_and_geometries(census, geometries)


def zone_agebs_factory(name: Literal["GHSL", "WORLDPOP"]) -> dg.AssetsDefinition:
    @dg.asset(
        name=name,
        key_prefix="agebs",
        ins={
            "census_agebs": dg.AssetIn(
                ["agebs", "base"], partition_mapping=dg.AllPartitionMapping()
            ),
            "mesh": dg.AssetIn([name, "mesh", "pop_fine"]),
        },
        partitions_def=year_and_zone_partitions,
        io_manager_key="gpkg_manager",
        group_name="agebs",
        tags={"concurrency": "limited"}
    )
    def _asset(
        mesh: gpd.GeoDataFrame, census_agebs: dict[str, gpd.GeoDataFrame]
    ) -> gpd.GeoDataFrame:
        return intersect_mesh_with_geoms(mesh, census_agebs, predicate="intersects").reset_index(drop=True).reset_index(names="ageb_idx")

    return _asset


dassets = [
    zone_agebs_factory("GHSL"),
    zone_agebs_factory("WORLDPOP"),
]