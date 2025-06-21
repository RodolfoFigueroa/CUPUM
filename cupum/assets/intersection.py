from typing import Literal

import geopandas as gpd

import dagster as dg
from cupum.partitions import year_and_zone_partitions


def intersect_polygons_factory(
    *, outer: list[str], inner: list[str], outer_idx_name: str, inner_pop_name: str
) -> dg.AssetsDefinition:
    if outer[0] != inner[0]:
        err = "outer and inner must have the same name"
        raise ValueError(err)
    name = outer[0]

    @dg.asset(
        name="intersect_polygons",
        key_prefix=name,
        ins={
            "outer": dg.AssetIn(outer),
            "inner": dg.AssetIn(inner),
        },
        partitions_def=year_and_zone_partitions,
        io_manager_key="gpkg_manager",
        group_name=name,
    )
    def _asset(outer: gpd.GeoDataFrame, inner: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        outer = outer.to_crs("EPSG:6372")
        inner = inner.to_crs("EPSG:6372").assign(orig_area=lambda x: x.geometry.area)

        overlay = inner.overlay(outer[[outer_idx_name, "geometry"]]).assign(
            new_area=lambda x: x["geometry"].area,
            area_frac=lambda x: x["new_area"] / x["orig_area"],
            pop_frac=lambda x: x[inner_pop_name] * x["area_frac"],
        )
        return (
            outer.set_index(outer_idx_name)
            .assign(inner_pop=overlay.groupby(outer_idx_name)["pop_frac"].sum())
            .reset_index()
        )

    return _asset


def intersect_points_factory(
    name: Literal[
        "GHSL_coarse",
        "WORLDPOP_coarse",
        "LANDSCAN_coarse",
        "GHSL_fine",
        "WORLDPOP_fine",
    ],
) -> dg.AssetsDefinition:
    @dg.asset(
        name="intersect_points",
        key_prefix=name,
        ins={
            "mesh": dg.AssetIn([name, "mesh", "pop"]),
            "geometries": dg.AssetIn([name, "houses"]),
        },
        partitions_def=year_and_zone_partitions,
        io_manager_key="gpkg_manager",
        group_name=name,
    )
    def _asset(
        mesh: gpd.GeoDataFrame, geometries: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        mesh = mesh.to_crs("EPSG:6372")
        geometries = geometries.to_crs("EPSG:6372")

        return (
            mesh.set_index("mesh_idx")
            .assign(
                inner_pop=(
                    mesh[["mesh_idx", "geometry"]]
                    .sjoin(
                        geometries[["POBTOT", "geometry"]],
                        how="inner",
                        predicate="contains",
                    )
                    .groupby("mesh_idx")["POBTOT"]
                    .sum()
                )
            )
            .fillna(0)
        )

    return _asset


def intersect_merged_factory(
    name: Literal["GHSL", "LANDSCAN", "WORLDPOP"],
) -> dg.AssetsDefinition:
    name_ext = f"{name}_coarse"

    @dg.asset(
        name="intersect_merged",
        key_prefix=name_ext,
        ins={
            "points": dg.AssetIn([name_ext, "intersect_points"]),
            "polygons": dg.AssetIn([name_ext, "intersect_polygons"]),
        },
        partitions_def=year_and_zone_partitions,
        io_manager_key="gpkg_manager",
        group_name=name_ext,
    )
    def _asset(
        points: gpd.GeoDataFrame, polygons: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        return (
            points.set_index("mesh_idx")
            .rename(columns={"inner_pop": "points_pop"})
            .assign(
                polygons_pop=(polygons.set_index("mesh_idx")["inner_pop"]),
                census_pop=lambda df: df["points_pop"] + df["polygons_pop"],
            )
            .drop(columns=["points_pop", "polygons_pop"])
            .rename(columns={"pop": "mesh_pop"})
        )

    return _asset


def intersect_merged_fine_factory(
    name: Literal["GHSL", "WORLDPOP"],
) -> dg.AssetsDefinition:
    name_ext = f"{name}_fine"

    @dg.asset(
        name="intersect_merged",
        key_prefix=name_ext,
        ins={
            "polygons": dg.AssetIn([name_ext, "intersect_polygons"]),
        },
        partitions_def=year_and_zone_partitions,
        io_manager_key="gpkg_manager",
        group_name=name_ext,
    )
    def _asset(polygons: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        return (
            polygons
            .rename(
                columns={"inner_pop": "mesh_pop", "POBTOT": "census_pop"}
            )
            .assign(
                mesh_minus_census=lambda df: df["mesh_pop"] - df["census_pop"],
                diff_abs=lambda df: df["mesh_minus_census"].abs()
            )
        )

    return _asset


dassets = [
    intersect_polygons_factory(
        outer=["GHSL_coarse", "mesh", "pop"],
        inner=["GHSL_coarse", "geometries"],
        outer_idx_name="mesh_idx",
        inner_pop_name="POBTOT",
    ),
    intersect_polygons_factory(
        outer=["WORLDPOP_coarse", "mesh", "pop"],
        inner=["WORLDPOP_coarse", "geometries"],
        outer_idx_name="mesh_idx",
        inner_pop_name="POBTOT",
    ),
    intersect_polygons_factory(
        outer=["LANDSCAN_coarse", "mesh", "pop"],
        inner=["LANDSCAN_coarse", "geometries"],
        outer_idx_name="mesh_idx",
        inner_pop_name="POBTOT",
    ),
    intersect_polygons_factory(
        outer=["GHSL_fine", "agebs"],
        inner=["GHSL_fine", "mesh", "pop"],
        outer_idx_name="ageb_idx",
        inner_pop_name="pop",
    ),
    intersect_polygons_factory(
        outer=["WORLDPOP_fine", "agebs"],
        inner=["WORLDPOP_fine", "mesh", "pop"],
        outer_idx_name="ageb_idx",
        inner_pop_name="pop",
    ),
    intersect_points_factory("GHSL_coarse"),
    intersect_points_factory("LANDSCAN_coarse"),
    intersect_points_factory("WORLDPOP_coarse"),
    intersect_merged_factory("GHSL"),
    intersect_merged_factory("LANDSCAN"),
    intersect_merged_factory("WORLDPOP"),
    intersect_merged_fine_factory("GHSL"),
    intersect_merged_fine_factory("WORLDPOP"),
]
