import dagster as dg
from cupum import assets
from cupum.managers import DataFrameIOManager, GeoDataFrameIOManager
from cupum.resources import PathResource

# Resources
path_resource = PathResource(
    data_path=dg.EnvVar("DATA_PATH"),
    ghsl_path=dg.EnvVar("GHSL_PATH"),
    worldpop_path=dg.EnvVar("WORLDPOP_PATH"),
    population_grids_path=dg.EnvVar("POPULATION_GRIDS_PATH"),
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
        "gpkg_manager": gpkg_manager,
        "csv_manager": csv_manager,
    },
)
