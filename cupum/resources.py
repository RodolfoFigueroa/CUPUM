from dagster import ConfigurableResource


class PathResource(ConfigurableResource):
    data_path: str
    population_grids_path: str
    geostatistical_framework_path: str


class RasterAttributesResource(ConfigurableResource):
    path: str
    nodata: int
    subpath: str
    subpath_fine: str
