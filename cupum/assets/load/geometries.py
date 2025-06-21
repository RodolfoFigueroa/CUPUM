from pathlib import Path
from typing import Literal

import geopandas as gpd
import pandas as pd

import dagster as dg
from cupum.constants import STATE_CODE_MAP
from cupum.resources import PathResource


def load_geometries_2010_factory(name: Literal["ageb", "block", "loc"], *, extra_col: str | None=None, rename_extra_col: str | None=None) -> dg.OpDefinition:
    if name == "block":
        suffix = "M"
    elif name == "loc":
        suffix = "L"
    elif name == "ageb":
        suffix = "A"
    else:
        err = f"Invalid name: {name}. Must be one of 'block' or 'loc'."
        raise ValueError(err)

    @dg.op(name=f"load_{name}_geometries_2010")
    def _op(
        context: dg.OpExecutionContext, path_resource: PathResource
    ) -> gpd.GeoDataFrame:
        state_code_map_inv = {v: k for k, v in STATE_CODE_MAP.items()}
        state_path = (
            Path(path_resource.geostatistical_framework_path)
            / "2010"
            / "descarga_manzanas"
            / "download"
            / state_code_map_inv[context.partition_key]
        )

        all_df = []
        for subdir in state_path.iterdir():
            for loc_dir in subdir.iterdir():
                for block_file in loc_dir.glob(f"*{suffix}.shp", case_sensitive=False):
                    df = gpd.read_file(block_file).to_crs("ESRI:54009")

                    if "CVEGEO" not in df.columns:
                        continue

                    if extra_col is None:
                        df = df[
                            ["CVEGEO", "geometry"]
                        ]
                    else:
                        if rename_extra_col is None:
                            err = f"Invalid extra_col: {extra_col}. Must be one of 'TIPOLOC' or 'TIPOMZA'."
                            raise ValueError(err)

                        df = df[
                            ["CVEGEO", extra_col, "geometry"]
                        ].rename(
                            columns={extra_col: rename_extra_col}
                        )
                    all_df.append(df)

        return gpd.GeoDataFrame(
            pd.concat(all_df, ignore_index=True), geometry="geometry", crs="ESRI:54009"
        ).set_index("CVEGEO")

    return _op


def load_geometries_2020_factory(
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

    @dg.op(name=f"load_{name}_geometries_2020")
    def _op(
        context: dg.OpExecutionContext, path_resource: PathResource
    ) -> gpd.GeoDataFrame:
        state = context.partition_key

        state_geometries_path = (
            Path(path_resource.geostatistical_framework_path) / "2020"
        )
        geom_path = next(state_geometries_path.glob(f"{state}*"))
        out = gpd.read_file(geom_path / f"{state}{suffix}.shp").set_index("CVEGEO")

        if extra_col is None:
            return out[["geometry"]]

        return out[[extra_col, "geometry"]]

    return _op


load_ageb_geometries_2010 = load_geometries_2010_factory("ageb")
load_block_geometries_2010 = load_geometries_2010_factory("block")
load_loc_geometries_2010 = load_geometries_2010_factory("loc", extra_col="TIPOLOC", rename_extra_col="AMBITO")

load_ageb_geometries_2020 = load_geometries_2020_factory("ageb")
load_loc_geometries_2020 = load_geometries_2020_factory("loc", extra_col="AMBITO")
load_block_geometries_2020 = load_geometries_2020_factory("block", extra_col="TIPOMZA")
load_house_geometries_2020 = load_geometries_2020_factory("house", extra_col="TIPOMZA")
