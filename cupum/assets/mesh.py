from pathlib import Path
from typing import Literal

import geopandas as gpd
import numpy as np
import rasterio as rio
import rasterio.features as rio_features
import rasterio.mask as rio_mask
import shapely

import dagster as dg
from cupum.partitions import year_and_zone_partitions, zone_partitions
from cupum.resources import PathResource, RasterAttributesResource


def load_agebs(zone: str, path_resource: PathResource) -> gpd.GeoDataFrame:
    agebs_path = (
        Path(path_resource.population_grids_path)
        / "final"
        / "zone_agebs"
        / "shaped"
        / "2020"
        / f"{zone}.gpkg"
    )
    return gpd.read_file(agebs_path)


def raw_mesh_factory(name: Literal["GHSL", "WORLDPOP"], mesh_type: Literal["coarse", "fine"]) -> dg.AssetsDefinition:
    if name == "GHSL":
        resource_key = "ghsl_attributes"
    elif name == "WORLDPOP":
        resource_key = "worldpop_attributes"

    @dg.asset(
        name=mesh_type,
        key_prefix=[name, "mesh"],
        partitions_def=zone_partitions,
        required_resource_keys={"path_resource", resource_key},
        io_manager_key="gpkg_manager",
        group_name=f"{name}_mesh_{mesh_type}",
    )
    def _asset(context: dg.AssetExecutionContext) -> gpd.GeoDataFrame:
        path_resource = context.resources.path_resource
        attributes_resource: RasterAttributesResource = getattr(
            context.resources, resource_key
        )

        df_agebs = load_agebs(context.partition_key, path_resource)

        if mesh_type == "coarse":
            raster_path = (
                Path(attributes_resource.path) / attributes_resource.subpath / "2020.tif"
            )
        elif mesh_type == "fine":
            raster_path = Path(attributes_resource.path) / attributes_resource.subpath_fine / "2020.tif"
        else:
            err = "Invalid mesh type"
            raise ValueError(err)

        with rio.open(raster_path, nodata=attributes_resource.nodata) as ds:
            crs = ds.crs
            df_agebs = df_agebs.to_crs(crs)

            xmin, ymin, xmax, ymax = df_agebs.total_bounds
            bbox = shapely.box(xmin, ymin, xmax, ymax)

            data, transform = rio_mask.mask(
                ds,
                [bbox],
                nodata=attributes_resource.nodata,
                crop=True,
            )
            data = data.squeeze()

        dummy = np.array(range(data.size)).reshape(data.shape).astype(np.int32)
        features = [
            shapely.geometry.shape(feature[0])
            for feature in rio_features.shapes(dummy, transform=transform)
        ]

        df_mesh = gpd.GeoDataFrame(geometry=features, crs=crs)

        joined = df_agebs[["geometry"]].sjoin(
            df_mesh[["geometry"]], how="inner", predicate="intersects"
        )
        wanted_idx = joined["index_right"].unique()

        return (
            df_mesh.loc[wanted_idx].reset_index(drop=True).reset_index(names="mesh_idx")
        )

    return _asset


def mesh_with_pop_factory(name: Literal["GHSL", "WORLDPOP"], mesh_type: Literal["coarse", "fine"]) -> dg.AssetsDefinition:
    if name == "GHSL":
        resource_key = "ghsl_attributes"
    elif name == "WORLDPOP":
        resource_key = "worldpop_attributes"

    @dg.asset(
        name=f"pop_{mesh_type}",
        key_prefix=[name, "mesh"],
        ins={"mesh": dg.AssetIn([name, "mesh", mesh_type])},
        required_resource_keys={resource_key},
        partitions_def=year_and_zone_partitions,
        io_manager_key="gpkg_manager",
        group_name=f"{name}_mesh_{mesh_type}",
    )
    def _asset(
        context: dg.AssetExecutionContext, mesh: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        year, _ = context.partition_key.split("|")

        attributes_resource: RasterAttributesResource = getattr(
            context.resources, resource_key
        )

        if mesh_type == "coarse":
            raster_path = (
                Path(attributes_resource.path) / attributes_resource.subpath / "2020.tif"
            )
        elif mesh_type == "fine":
            raster_path = Path(attributes_resource.path) / attributes_resource.subpath_fine / "2020.tif"
        else:
            err = "Invalid mesh type"
            raise ValueError(err)
            
        with rio.open(raster_path) as ds:
            centroids = (
                mesh.to_crs("EPSG:6372")
                .centroid.to_crs(ds.crs)
                .get_coordinates()
                .to_numpy()
            )
            pops = np.array(list(ds.sample(centroids, masked=True))).squeeze()
            pops[pops == ds.nodata] = np.nan

        return mesh.assign(pop=pops)

    return _asset


dassets = [
    raw_mesh_factory("GHSL", "coarse"), 
    raw_mesh_factory("WORLDPOP", "coarse"),
    raw_mesh_factory("GHSL", "fine"), 
    raw_mesh_factory("WORLDPOP", "fine"),
    
    mesh_with_pop_factory("GHSL", "coarse"),
    mesh_with_pop_factory("WORLDPOP", "coarse"),
    mesh_with_pop_factory("GHSL", "fine"),
    mesh_with_pop_factory("WORLDPOP", "fine"),
]
