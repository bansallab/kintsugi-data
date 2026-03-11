from typing import NamedTuple

import polars as pl

from .globals import DATA, DATA_UNTRACKED


class FileInfo(NamedTuple):
    lb: int
    year_lb: int
    ub: int
    year_ub: int


def main() -> int:
    year_bounds = {
        2016: FileInfo(3, 2010, 9, 2016),
        2017: FileInfo(3, 2010, 10, 2017),
        2018: FileInfo(3, 2010, 11, 2018),
        2019: FileInfo(3, 2010, 12, 2019),
        2020: FileInfo(3, 2010, 13, 2020),
        2021: FileInfo(2, 2020, 3, 2021),
        2022: FileInfo(2, 2020, 4, 2022),
        2023: FileInfo(2, 2020, 5, 2023),
        2024: FileInfo(2, 2020, 6, 2024),
    }
    age_grps = {
        0: "tot",
        1: "0-4",
        2: "5-9",
        3: "10-14",
        4: "15-19",
        5: "20-24",
        6: "25-29",
        7: "30-34",
        8: "35-39",
        9: "40-44",
        10: "45-49",
        11: "50-54",
        12: "55-59",
        13: "60-64",
        14: "65-69",
        15: "70-74",
        16: "75-79",
        17: "80-84",
        18: ">=85",
    }
    age_grp_enum = pl.Enum(age_grps.values())

    for file in sorted((DATA_UNTRACKED / "pop/county_cc").glob("*.csv")):
        year = int(file.name.split("-")[1][3:])
        year_info = year_bounds[year]
        lf = (
            pl.scan_csv(
                file,
                encoding="utf8-lossy",
                null_values=["X"],
                new_columns=[
                    "summary_lvl",
                    "state_fips",
                    "county_fips",
                    "state_name",
                    "county_name",
                    "year",
                    "age_grp",
                    "tot_pop",
                    "tot_male",
                    "tot_female",
                    "white_male",
                    "white_female",
                    "black_male",
                    "black_female",
                    "aian_male",
                    "aian_female",
                    "asian_male",
                    "asian_female",
                    "nhpi_male",
                    "nhpi_female",
                ],
                schema_overrides={
                    "summary_lvl": pl.String,
                    "state_fips": pl.String,
                    "county_fips": pl.String,
                    "state_name": pl.String,
                    "county_name": pl.String,
                    "year": pl.Int64,
                    "age_grp": pl.Int64,
                    "tot_pop": pl.Int64,
                    "tot_male": pl.Int64,
                    "tot_female": pl.Int64,
                    "white_male": pl.Int64,
                    "white_female": pl.Int64,
                    "black_male": pl.Int64,
                    "black_female": pl.Int64,
                    "aian_male": pl.Int64,
                    "aian_female": pl.Int64,
                    "asian_male": pl.Int64,
                    "asian_female": pl.Int64,
                    "nhpi_male": pl.Int64,
                    "nhpi_female": pl.Int64,
                    "H_MALE": pl.Int64,
                    "H_FEMALE": pl.Int64,
                },
            )
            .rename({"H_MALE": "hispanic_male", "H_FEMALE": "hispanic_female"})
            .select(
                "summary_lvl",
                "state_fips",
                "county_fips",
                "state_name",
                "county_name",
                "year",
                "age_grp",
                "tot_pop",
                "tot_male",
                "tot_female",
                "white_male",
                "white_female",
                "black_male",
                "black_female",
                "aian_male",
                "aian_female",
                "asian_male",
                "asian_female",
                "nhpi_male",
                "nhpi_female",
                "hispanic_male",
                "hispanic_female",
            )
            .filter(
                pl.col("summary_lvl") == "050",
                pl.col("state_fips").is_between(pl.lit("01"), pl.lit("56")),
                pl.col("year").is_between(year_info.lb, year_info.ub),
            )
            .with_columns(
                county_fips=pl.col("state_fips") + pl.col("county_fips"),
                year=pl.col("year").replace_strict(
                    {
                        k: v
                        for k, v in zip(
                            range(year_info.lb, year_info.ub + 1),
                            range(year_info.year_lb, year_info.year_ub + 1),
                        )
                    },
                    return_dtype=pl.Int64,
                ),
                age_grp=pl.col("age_grp")
                .replace_strict(age_grps, return_dtype=pl.String)
                .cast(age_grp_enum),
            )
            .drop("summary_lvl", "state_fips")
            .sort("county_fips", "year", "age_grp")
        )

        lf.sink_parquet(DATA / f"pop/county_cc/county_pop_{year}.parquet")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
