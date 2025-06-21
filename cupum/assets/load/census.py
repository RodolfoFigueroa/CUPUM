from pathlib import Path
from typing import Literal

import pandas as pd

import dagster as dg
from cupum.resources import PathResource


def load_census_loc_factory(year: int) -> dg.OpDefinition:
    @dg.op(name=f"census_loc_{year}")
    def _op(
        context: dg.OpExecutionContext, path_resource: PathResource
    ) -> pd.DataFrame:
        state = context.partition_key

        iter_census_path = (
            Path(path_resource.population_grids_path) / "initial" / "census" / "ITER"
        )

        if year == 2010:
            fpath = iter_census_path / "ITER_NALDBF10.csv"
        elif year == 2020:
            fpath = iter_census_path / "ITER_NALCSV20.csv"
        else:
            err = f"Invalid year: {year}. Must be one of 2010 or 2020."
            raise ValueError(err)

        return (
            pd.read_csv(
                fpath,
                usecols=["ENTIDAD", "MUN", "LOC", "POBTOT"],
            )
            .assign(ENTIDAD=lambda df: df["ENTIDAD"].astype(str).str.zfill(2))
            .query(f"ENTIDAD == '{state}'")
            .assign(
                CVEGEO=lambda df: (
                    df["ENTIDAD"].astype(str).str.rjust(2, "0")
                    + df["MUN"].astype(str).str.rjust(3, "0")
                    + df["LOC"].astype(str).str.rjust(4, "0")
                )
            )
            .set_index("CVEGEO")[["POBTOT"]]
        )

    return _op


def load_census_factory(name: Literal["block", "ageb"], year: int) -> dg.OpDefinition:
    @dg.op(name=f"load_{name}_census_{year}")
    def _op(
        context: dg.OpExecutionContext, path_resource: PathResource
    ) -> pd.DataFrame:
        state = context.partition_key

        state_census_path = (
            Path(path_resource.population_grids_path)
            / "initial"
            / "census"
            / "INEGI"
            / str(year)
        )

        cols = ["ENTIDAD", "MUN", "LOC", "AGEB", "MZA", "POBTOT", "NOM_LOC"]
        if year == 2010:
            fpath = state_census_path / f"resultados_ageb_urbana_{state}_cpv2010.csv"
            cols = [col.lower() for col in cols]
        elif year == 2020:
            fpath = (
                state_census_path / f"conjunto_de_datos_ageb_urbana_{state}_cpv2020.csv"
            )
        else:
            err = f"Invalid year: {year}. Must be one of 2010 or 2020."
            raise ValueError(err)

        out = pd.read_csv(
            fpath,
            usecols=cols,
        )
        out.columns = [c.upper() for c in out.columns]

        if name == "ageb":
            out = out.query("NOM_LOC == 'Total AGEB urbana'")

        out = out.assign(
            CVEGEO=lambda df: (
                df["ENTIDAD"].astype(str).str.rjust(2, "0")
                + df["MUN"].astype(str).str.rjust(3, "0")
                + df["LOC"].astype(str).str.rjust(4, "0")
                + df["AGEB"].astype(str).str.rjust(4, "0")
            )
        )

        if name == "block":
            out = out.assign(
                CVEGEO=lambda df: df["CVEGEO"] + df["MZA"].astype(str).str.rjust(3, "0")
            )

        return out.set_index("CVEGEO")[["POBTOT"]]

    return _op


load_census_loc_2010 = load_census_loc_factory(2010)
load_census_loc_2020 = load_census_loc_factory(2020)

load_census_blocks_and_houses_2010 = load_census_factory("block", year=2010)
load_census_blocks_and_houses_2020 = load_census_factory("block", year=2020)

load_census_agebs_2010 = load_census_factory("ageb", year=2010)
load_census_agebs_2020 = load_census_factory("ageb", year=2020)
