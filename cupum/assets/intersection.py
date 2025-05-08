from typing import Literal

import geopandas as gpd

import dagster as dg

from cupum.partitions import zone_partitions


def intersect_polygons_factory(
    name: Literal["GHSL", "WORLDPOP"],
) -> dg.AssetsDefinition:
    @dg.asset(
        name="intersect_polygons",
        key_prefix=name,
        ins={
            "mesh": dg.AssetIn([name, "mesh", "pop"]),
            "geometries": dg.AssetIn([name, "geometries"]),
        },
        partitions_def=zone_partitions,
        io_manager_key="gpkg_manager",
        group_name=name,
    )
    def _asset(
        mesh: gpd.GeoDataFrame, geometries: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        mesh = mesh.to_crs("EPSG:6372")
        geometries = geometries.to_crs("EPSG:6372").assign(orig_area=lambda x: x.geometry.area)

        overlay = geometries.overlay(
            mesh[["mesh_idx", "geometry"]]
        ).assign(
            new_area=lambda x: x.geometry.area,
            area_frac=lambda x: x.new_area / x.orig_area,
            pop_frac=lambda x: x.POBTOT * x.area_frac,
        )
        return (
            mesh
            .set_index("mesh_idx")
            .assign(census_pop=overlay.groupby("mesh_idx")["pop_frac"].sum())
            .reset_index()
        )

    return _asset


def intersect_points_factory(name: Literal["GHSL", "WORLDPOP"]) -> dg.AssetsDefinition:
    @dg.asset(
        name="intersect_points",
        key_prefix=name,
        ins={
            "mesh": dg.AssetIn([name, "mesh", "pop"]),
            "geometries": dg.AssetIn([name, "houses"]),
        },
        partitions_def=zone_partitions,
        io_manager_key="gpkg_manager",
        group_name=name,
    )
    def _asset(
        mesh: gpd.GeoDataFrame, geometries: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        return (
            mesh
            .set_index("mesh_idx")
            .assign(
                census_pop=(
                    mesh[["mesh_idx", "geometry"]]
                    .sjoin(geometries[["POBTOT", "geometry"]], how="inner", predicate="contains")
                    .groupby("mesh_idx")
                    ["POBTOT"]
                    .sum()
                )
            )
            .fillna(0)
        )

    return _asset


def intersect_merged_factory(name: Literal["GHSL", "WORLDPOP"]) -> dg.AssetsDefinition:
    @dg.asset(
        name="intersect_merged",
        key_prefix=name,
        ins={
            "points": dg.AssetIn([name, "intersect_points"]),
            "polygons": dg.AssetIn([name, "intersect_polygons"]),
        },
        partitions_def=zone_partitions,
        io_manager_key="gpkg_manager",
        group_name=name,
    )
    def _asset(
        points: gpd.GeoDataFrame, polygons: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        return (
            points
            .set_index("mesh_idx")
            .rename(columns={"census_pop": "points_pop"})
            .assign(
                polygons_pop=(
                    polygons.set_index("mesh_idx")["census_pop"]
                ),
                census_pop=lambda df: df["points_pop"] + df["polygons_pop"]
            )
            .drop(columns=["points_pop", "polygons_pop"])
        )

    return _asset


dassets = [intersect_polygons_factory("GHSL"), intersect_polygons_factory("WORLDPOP"), intersect_points_factory("GHSL"), intersect_points_factory("WORLDPOP"), intersect_merged_factory("GHSL"), intersect_merged_factory("WORLDPOP")]  
