from typing import Literal

import geopandas as gpd
import pandas as pd

import dagster as dg
from cupum.partitions import year_partitions


def stats_factory(
    name: Literal["GHSL", "WORLDPOP", "LANDSCAN"],
    mesh_type: Literal["coarse", "fine"],
) -> dg.AssetsDefinition:
    name_ext = f"{name}_{mesh_type}"

    @dg.asset(
        name="stats",
        key_prefix=name_ext,
        ins={"intersect_merged": dg.AssetIn([name_ext, "intersect_merged"])},
        partitions_def=year_partitions,
        io_manager_key="csv_manager",
        group_name=name_ext,
    )
    def _asset(intersect_merged: dict[str, gpd.GeoDataFrame]) -> pd.DataFrame:
        stats_df = []
        for key, df in intersect_merged.items():
            _, zone = key.split("|")
            pop_mesh = df["mesh_pop"].sum()
            pop_census = df["census_pop"].sum()
            stats_df.append(
                {
                    "zone": zone,
                    "pop_mesh": pop_mesh,
                    "pop_census": pop_census,
                }
            )
        return pd.DataFrame(stats_df).set_index("zone")

    return _asset


dassets = [
    stats_factory("GHSL", mesh_type="coarse"),
    stats_factory("GHSL", mesh_type="fine"),
    stats_factory("LANDSCAN", mesh_type="coarse"),
    stats_factory("WORLDPOP", mesh_type="coarse"),
    stats_factory("WORLDPOP", mesh_type="fine"),
]
