from pathlib import Path

import geopandas as gpd
import pandas as pd

import dagster as dg
from cupum.assets.common import load_loc_geometries, merge_census_and_geometries
from cupum.partitions import state_and_year_partitions
from cupum.resources import PathResource


@dg.op
def load_census_loc(
    context: dg.OpExecutionContext, path_resource: PathResource
) -> pd.DataFrame:
    state, _ = context.partition_key.split("|")

    iter_census_path = (
        Path(path_resource.population_grids_path)
        / "initial"
        / "census"
        / "ITER"
        / "ITER_NALCSV20.csv"
    )

    return (
        pd.read_csv(
            iter_census_path,
            usecols=["ENTIDAD", "MUN", "LOC", "POBTOT"],
        )
        .assign(ENTIDAD=lambda df: df["ENTIDAD"].astype(str).str.zfill(2))
        .query(f"ENTIDAD == '{state}'")
        .assign(
            CVEGEO=lambda df: (
                df["ENTIDAD"].astype(str).str.rjust(2, "0")
                + df["MUN"].astype(str).str.rjust(3, "0")
                + df["LOC"].astype(str).str.rjust(4, "0")
            )
        )
        .set_index("CVEGEO")[["POBTOT"]]
    )


@dg.graph_asset(
    name="base",
    key_prefix="locs",
    partitions_def=state_and_year_partitions,
    group_name="locs",
)
def locs() -> gpd.GeoDataFrame:
    census = load_census_loc()
    geometries = load_loc_geometries()
    return merge_census_and_geometries(census, geometries)
