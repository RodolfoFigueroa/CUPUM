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
    path=dg.EnvVar("GHSL_PATH"), subpath="POP_1000", subpath_fine="POP_100"
)
landscan_attributes = RasterAttributesResource(
    path=dg.EnvVar("LANDSCAN_PATH"), subpath="GLOBAL", subpath_fine=None
)
worldpop_attributes = RasterAttributesResource(
    path=dg.EnvVar("WORLDPOP_PATH"), subpath="1000", subpath_fine="100"
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
                    assets.geometries,
                    assets.intersection,
                    assets.zoned,
                    assets.merged_years,
                    assets.mesh,
                    assets.stats,
                ]
            )
        )
        + list(dg.load_assets_from_package_module(assets.blocks))
        + list(dg.load_assets_from_package_module(assets.houses))
        + list(dg.load_assets_from_package_module(assets.locs))
        + list(dg.load_assets_from_package_module(assets.load))
        + list(dg.load_assets_from_package_module(assets.agebs))
    ),
    resources={
        "landscan_attributes": landscan_attributes,
        "path_resource": path_resource,
        "worldpop_attributes": worldpop_attributes,
        "ghsl_attributes": ghsl_attributes,
        "gpkg_manager": gpkg_manager,
        "csv_manager": csv_manager,
    },
)
