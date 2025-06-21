import geopandas as gpd

import dagster as dg
from cupum.partitions import state_and_year_partitions


def merged_years_factory(
    asset_2010: list[str], asset_2020: list[str]
) -> dg.AssetsDefinition:
    if asset_2010[0] != asset_2020[0]:
        err = "The assets must have the same prefix."
        raise ValueError(err)

    name = asset_2010[0]

    @dg.asset(
        name="base",
        key_prefix=name,
        ins={
            "gdf_2010": dg.AssetIn(asset_2010),
            "gdf_2020": dg.AssetIn(asset_2020),
        },
        partitions_def=state_and_year_partitions,
        io_manager_key="gpkg_manager",
        group_name=name,
    )
    def _asset(
        context: dg.AssetExecutionContext,
        gdf_2010: gpd.GeoDataFrame,
        gdf_2020: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        _, year = context.partition_key.split("|")
        if year == "2010":
            return gdf_2010
        if year == "2020":
            return gdf_2020
        err = f"Invalid year: {year}. Must be one of '2010' or '2020'."
        raise ValueError(err)

    return _asset


dassets = [
    merged_years_factory(["agebs", "2010"], ["agebs", "2020"]),
    merged_years_factory(["blocks", "cut_2010"], ["blocks", "cut_2020"]),
    merged_years_factory(["locs", "2010"], ["locs", "2020"]),
    merged_years_factory(["houses", "2010"], ["houses", "2020"]),
]
