from pathlib import Path
from typing import Literal

import geopandas as gpd
import pandas as pd

import dagster as dg
from cupum.resources import PathResource


def intersect_mesh_with_geoms(
    mesh: gpd.GeoDataFrame,
    geoms: dict[str, gpd.GeoDataFrame],
    *,
    predicate: Literal["intersects", "within"],
) -> gpd.GeoDataFrame:
    crs = next(iter(geoms.values())).crs

    if crs is None:
        err = "CRS is None, cannot perform intersection."
        raise ValueError(err)

    mesh = mesh.to_crs(crs)

    df_out = []
    for df in geoms.values():
        temp = df.sjoin(mesh[["geometry"]], how="inner", predicate=predicate).drop(
            columns=["index_right"]
        )
        df_out.append(temp)

    return (
        gpd.GeoDataFrame(pd.concat(df_out, ignore_index=True), crs=crs)
        .drop_duplicates(subset=["CVEGEO"])
        .reset_index(drop=True)
    )


@dg.op(out=dg.Out(io_manager_key="gpkg_manager"))
def merge_census_and_geometries(
    census: pd.DataFrame, geometries: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    return geometries.join(census, how="inner").to_crs("ESRI:54009")


def load_census_factory(name: Literal["block", "ageb"]) -> dg.OpDefinition:
    @dg.op(name=f"load_{name}_census")
    def _op(
        context: dg.OpExecutionContext, path_resource: PathResource
    ) -> pd.DataFrame:
        state, _ = context.partition_key.split("|")

        state_census_path = (
            Path(path_resource.population_grids_path)
            / "initial"
            / "census"
            / "INEGI"
            / "2020"
        )

        out = pd.read_csv(
            state_census_path / f"conjunto_de_datos_ageb_urbana_{state}_cpv2020.csv",
            usecols=["ENTIDAD", "MUN", "LOC", "AGEB", "MZA", "POBTOT"],
        ).assign(
            CVEGEO=lambda df: (
                df["ENTIDAD"].astype(str).str.rjust(2, "0")
                + df["MUN"].astype(str).str.rjust(3, "0")
                + df["LOC"].astype(str).str.rjust(4, "0")
                + df["AGEB"].astype(str).str.rjust(4, "0")
            )
        )

        if name == "block":
            out.assign(
                CVEGEO=lambda df: df["CVEGEO"] + df["MZA"].astype(str).str.rjust(3, "0")
            )

        return out.set_index("CVEGEO")[["POBTOT"]]

    return _op


def load_geometries_factory(
    name: Literal["block", "house", "loc", "ageb"], *, extra_col: str | None = None
) -> dg.OpDefinition:
    if name == "block":
        suffix = "m"
    elif name == "house":
        suffix = "cd"
    elif name == "loc":
        suffix = "l"
    elif name == "ageb":
        suffix = "a"
    else:
        err = f"Invalid name: {name}. Must be one of 'block', 'house', or 'loc'."
        raise ValueError(err)

    @dg.op(name=f"load_{name}_geometries")
    def _op(
        context: dg.OpExecutionContext, path_resource: PathResource
    ) -> gpd.GeoDataFrame:
        state, _ = context.partition_key.split("|")

        state_geometries_path = (
            Path(path_resource.geostatistical_framework_path) / "2020"
        )
        geom_path = next(state_geometries_path.glob(f"{state}*"))
        out = gpd.read_file(geom_path / f"{state}{suffix}.shp").set_index("CVEGEO")

        if extra_col is None:
            return out[["geometry"]]
        return out[[extra_col, "geometry"]]

    return _op


load_ageb_geometries = load_geometries_factory("ageb")
load_loc_geometries = load_geometries_factory("loc", extra_col="AMBITO")
load_block_geometries = load_geometries_factory("block", extra_col="TIPOMZA")
load_house_geometries = load_geometries_factory("house", extra_col="TIPOMZA")

load_census_blocks_and_houses = load_census_factory("block")
load_census_agebs = load_census_factory("ageb")
