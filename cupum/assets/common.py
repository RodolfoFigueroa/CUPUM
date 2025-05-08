from pathlib import Path
from typing import Literal

import geopandas as gpd
import pandas as pd

import dagster as dg
from cupum.resources import PathResource


def generate_attributes(name: Literal["GHSL", "WORLDPOP"]) -> dict:
    if name == "GHSL":
        out = {
            "attr": "ghsl_path",
            "subpath": "POP_1000/2020.tif",
            "nodata": -200,
        }
    elif name == "WORLDPOP":
        out = {
            "attr": "worldpop_path",
            "subpath": "1000/2020.tif",
            "nodata": -99999,
        }
    return out


@dg.op(out=dg.Out(io_manager_key="gpkg_manager"))
def merge_census_and_geometries(
    census: pd.DataFrame, geometries: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    return geometries.join(census, how="inner").to_crs("ESRI:54009")


def load_agebs(
    context: dg.AssetExecutionContext, path_resource: PathResource
) -> gpd.GeoDataFrame:
    agebs_path = (
        Path(path_resource.population_grids_path)
        / "final"
        / "zone_agebs"
        / "shaped"
        / "2020"
        / f"{context.partition_key}.gpkg"
    )
    return gpd.read_file(agebs_path)


@dg.op
def load_census_blocks_and_houses(
    context: dg.OpExecutionContext, path_resource: PathResource
) -> pd.DataFrame:
    state_census_path = (
        Path(path_resource.population_grids_path)
        / "initial"
        / "census"
        / "INEGI"
        / "2020"
    )

    return (
        pd.read_csv(
            state_census_path
            / f"conjunto_de_datos_ageb_urbana_{context.partition_key}_cpv2020.csv",
            usecols=["ENTIDAD", "MUN", "LOC", "AGEB", "MZA", "POBTOT"],
        )
        .assign(
            CVEGEO=lambda df: (
                df["ENTIDAD"].astype(str).str.rjust(2, "0")
                + df["MUN"].astype(str).str.rjust(3, "0")
                + df["LOC"].astype(str).str.rjust(4, "0")
                + df["AGEB"].astype(str).str.rjust(4, "0")
                + df["MZA"].astype(str).str.rjust(3, "0")
            )
        )
        .set_index("CVEGEO")[["POBTOT"]]
    )
