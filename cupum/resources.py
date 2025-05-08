from dagster import ConfigurableResource


class PathResource(ConfigurableResource):
    data_path: str
    ghsl_path: str
    population_grids_path: str
    worldpop_path: str
