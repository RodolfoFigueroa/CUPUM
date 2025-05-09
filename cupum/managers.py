from pathlib import Path

import geopandas as gpd
import pandas as pd

import dagster as dg
from cupum.resources import PathResource


def process_single_partition_key(
    partition_key: str, *, root_path: Path, extension: str
) -> Path:
    subkeys = partition_key.split("|")
    if len(subkeys) == 1:
        final_path = root_path / partition_key
    else:
        final_path = root_path / "/".join(subkeys)
    return final_path.with_suffix(final_path.suffix + extension)


class BaseManager(dg.ConfigurableIOManager):
    path_resource: dg.ResourceDependency[PathResource]
    extension: str

    def _get_path(
        self,
        context: dg.InputContext | dg.OutputContext,
    ) -> Path | dict[str, Path]:
        out_path = Path(self.path_resource.data_path) / "generated"
        fpath = out_path / "/".join(context.asset_key.path)

        if context.has_asset_partitions:
            if len(context.asset_partition_keys) == 1:
                final_path = process_single_partition_key(
                    context.asset_partition_key,
                    root_path=fpath,
                    extension=self.extension,
                )
            else:
                final_path = {
                    key: process_single_partition_key(
                        key, root_path=fpath, extension=self.extension
                    )
                    for key in context.asset_partition_keys
                }
        else:
            final_path = fpath.with_suffix(fpath.suffix + self.extension)

        return final_path


class DataFrameIOManager(BaseManager):
    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame) -> None:
        out_path = self._get_path(context)
        if isinstance(out_path, dict):
            err = "Output path is a dictionary, but DataFrameIOManager only supports single output."  # noqa: E501
            raise TypeError(err)

        out_path.parent.mkdir(exist_ok=True, parents=True)
        obj.to_csv(out_path)

    def load_input(
        self, context: dg.InputContext
    ) -> pd.DataFrame | dict[str, pd.DataFrame]:
        path = self._get_path(context)

        if isinstance(path, Path):
            return pd.read_csv(path)

        if isinstance(path, dict):
            return {key: pd.read_csv(fpath) for key, fpath in path.items()}

        err = "Input path is neither a Path nor a dictionary."
        raise TypeError(err)


class GeoDataFrameIOManager(BaseManager):
    def handle_output(self, context: dg.OutputContext, obj: gpd.GeoDataFrame) -> None:
        out_path = self._get_path(context)
        if isinstance(out_path, dict):
            err = "Output path is a dictionary, but DataFrameIOManager only supports single output."  # noqa: E501
            raise TypeError(err)

        out_path.parent.mkdir(exist_ok=True, parents=True)
        obj.to_file(out_path, mode="w")

    def load_input(
        self, context: dg.InputContext
    ) -> gpd.GeoDataFrame | dict[str, gpd.GeoDataFrame]:
        path = self._get_path(context)
        if isinstance(path, Path):
            return gpd.read_file(path)

        if isinstance(path, dict):
            return {key: gpd.read_file(fpath) for key, fpath in path.items()}

        err = "Input path is neither a Path nor a dictionary."
        raise TypeError(err)
