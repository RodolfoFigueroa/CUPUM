
import geopandas as gpd
import pandas as pd

import dagster as dg
from cupum.assets.common import load_census_blocks_and_houses, load_house_geometries
from cupum.partitions import state_and_year_partitions


@dg.op(out=dg.Out(io_manager_key="gpkg_manager"))
def merge_houses_census_and_geometries(
    geometries: gpd.GeoDataFrame, census: pd.DataFrame
) -> gpd.GeoDataFrame:
    df_houses = (
        geometries.join(census, how="inner")
        .to_crs("ESRI:54009")
        .reset_index(names="CVEGEO")
        .assign(
            POBTOT=lambda df: df["POBTOT"]
            / df["geometry"].apply(lambda x: len(x.geoms)),
        )
        .explode()
        .reset_index(drop=True)
    )

    group = df_houses.groupby("CVEGEO")
    mask = group["CVEGEO"].transform("count") == 1
    df_houses["CVEGEO_mod"] = df_houses["CVEGEO"] + group.cumcount().add(1).astype(
        str
    ).radd("_").mask(mask, "")

    return df_houses.drop(columns=["CVEGEO"]).rename(columns={"CVEGEO_mod": "CVEGEO"})


@dg.graph_asset(
    name="base",
    key_prefix="houses",
    partitions_def=state_and_year_partitions,
    group_name="houses",
)
def houses() -> gpd.GeoDataFrame:
    geometries = load_house_geometries()
    census = load_census_blocks_and_houses()
    return merge_houses_census_and_geometries(geometries, census)
