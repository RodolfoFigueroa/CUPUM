from pathlib import Path
from typing import Literal

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio
import rasterio.features as rio_features
import rasterio.mask as rio_mask
import shapely

import dagster as dg
from cupum.assets.common import generate_attributes, load_agebs
from cupum.partitions import zone_partitions
from cupum.resources import PathResource


def raw_mesh_factory(name: Literal["GHSL", "WORLDPOP"]) -> dg.AssetsDefinition:
    attributes = generate_attributes(name)

    @dg.asset(
        name="base",
        key_prefix=[name, "mesh"],
        partitions_def=zone_partitions,
        io_manager_key="gpkg_manager",
        group_name=f"{name}",
    )
    def _asset(
        context: dg.AssetExecutionContext, path_resource: PathResource
    ) -> gpd.GeoDataFrame:
        df_agebs = load_agebs(context, path_resource)

        base_path = Path(getattr(path_resource, attributes["attr"]))
        raster_path = base_path / attributes["subpath"]

        with rio.open(raster_path, nodata=attributes["nodata"]) as ds:
            crs = ds.crs
            df_agebs = df_agebs.to_crs(crs)

            xmin, ymin, xmax, ymax = df_agebs.total_bounds
            bbox = shapely.box(xmin, ymin, xmax, ymax)

            data, transform = rio_mask.mask(
                ds,
                [bbox],
                nodata=attributes["nodata"],
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

        return df_mesh.loc[wanted_idx].reset_index(drop=True).reset_index(names="mesh_idx")

    return _asset


def mesh_with_pop_factory(name: Literal["GHSL", "WORLDPOP"]) -> dg.AssetsDefinition:
    @dg.asset(
        name="pop",
        key_prefix=[name, "mesh"],
        ins={"mesh": dg.AssetIn([name, "mesh", "base"])},
        partitions_def=zone_partitions,
        io_manager_key="gpkg_manager",
        group_name=f"{name}",
    )
    def _asset(path_resource: PathResource, mesh: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        raster_path = Path(path_resource.ghsl_path) / "POP_1000" / "2020.tif"

        pops = []
        with rio.open(raster_path) as ds:
            mesh = mesh.to_crs(ds.crs)
            for idx, geom in mesh["geometry"].items():
                masked, _ = rio_mask.mask(ds, [geom], nodata=ds.nodata, crop=True)
                masked: np.ndarray
                if masked.size != 1:
                    err = (
                        "Raster has more than one value. Please check the data source."
                    )
                    raise ValueError(err)

                pops.append({"index": idx, "pop": masked.item()})

        pops = pd.DataFrame(pops).set_index("index")["pop"]
        return mesh.assign(pop=pops)

    return _asset


dassets = [raw_mesh_factory("GHSL"), raw_mesh_factory("WORLDPOP")] + [
    mesh_with_pop_factory("GHSL"),
    mesh_with_pop_factory("WORLDPOP"),
]
