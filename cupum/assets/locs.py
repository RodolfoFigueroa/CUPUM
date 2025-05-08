from pathlib import Path

import geopandas as gpd
import pandas as pd

import dagster as dg
from cupum.assets.common import merge_census_and_geometries
from cupum.partitions import state_partitions
from cupum.resources import PathResource


@dg.op
def load_loc_geometries(
    context: dg.OpExecutionContext, path_resource: PathResource
) -> gpd.GeoDataFrame:
    state_geometries_path = (
        Path(path_resource.population_grids_path) / "initial" / "geometry" / "states"
    )
    geom_path = next(state_geometries_path.glob(f"{context.partition_key}*"))
    return gpd.read_file(geom_path / f"{context.partition_key}l.shp").set_index(
        "CVEGEO"
    )[["AMBITO", "geometry"]]


@dg.op
def load_census_loc(
    context: dg.OpExecutionContext, path_resource: PathResource
) -> pd.DataFrame:
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
        .assign(ENTIDAD=lambda df: df["ENTIDAD"].astype(str).str.rjust(2, "0"))
        .query(f"ENTIDAD == '{context.partition_key}'")
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
    name="base", key_prefix="locs", partitions_def=state_partitions, group_name="locs"
)
def locs() -> gpd.GeoDataFrame:
    census = load_census_loc()
    geometries = load_loc_geometries()
    return merge_census_and_geometries(census, geometries)
