import dagster as dg
from cupum import assets
from cupum.managers import DataFrameIOManager, GeoDataFrameIOManager
from cupum.resources import PathResource, RasterAttributesResource

# Resources
path_resource = PathResource(
    data_path=dg.EnvVar("DATA_PATH"),
    population_grids_path=dg.EnvVar("POPULATION_GRIDS_PATH"),
    geostatistical_framework_path=dg.EnvVar("GEOSTATISTICAL_FRAMEWORK_PATH"),
)
ghsl_attributes = RasterAttributesResource(
    path=dg.EnvVar("GHSL_PATH"), nodata=-200, subpath="POP_1000", subpath_fine="POP_100"
)
worldpop_attributes = RasterAttributesResource(
    path=dg.EnvVar("WORLDPOP_PATH"), nodata=-99999, subpath="1000", subpath_fine="100"
)

# Managers
csv_manager = DataFrameIOManager(path_resource=path_resource, extension=".csv")
gpkg_manager = GeoDataFrameIOManager(path_resource=path_resource, extension=".gpkg")


# Definitions
defs = dg.Definitions(
    assets=(
        list(
            dg.load_assets_from_modules(
                [
                    assets.agebs,
                    assets.blocks,
                    assets.geometries,
                    assets.houses,
                    assets.intersection,
                    assets.zoned,
                    assets.locs,
                    assets.mesh,
                ]
            )
        )
    ),
    resources={
        "path_resource": path_resource,
        "worldpop_attributes": worldpop_attributes,
        "ghsl_attributes": ghsl_attributes,
        "gpkg_manager": gpkg_manager,
        "csv_manager": csv_manager,
    },
)
