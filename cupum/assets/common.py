from typing import Literal

import geopandas as gpd
import pandas as pd

import dagster as dg


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
    print(census)
    print(geometries)
    return geometries.join(census, how="inner").to_crs("ESRI:54009")
