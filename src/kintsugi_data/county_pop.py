import polars as pl

from .globals import DATA, DATA_UNTRACKED


def main() -> int:
    year_lb = 2010
    year_ub = 2024
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

    for year in range(2010, year_ub + 1):
        if year_lb <= year <= 2019:
            file = "pop/county_cc/CC-EST2020-ALLDATA.csv"
        else:
            file = "pop/county_cc/cc-est2024-alldata.csv"

        lf = (
            pl.scan_csv(
                DATA_UNTRACKED / file,
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
                },
            )
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
            )
            .filter(
                pl.col("summary_lvl") == "050",
                pl.col("state_fips").is_between(pl.lit("01"), pl.lit("56")),
                pl.col("year").is_between(3, 12)
                if year_lb <= year <= 2019
                else pl.col("year").is_between(2, 6),
            )
            .with_columns(
                county_fips=pl.col("state_fips") + pl.col("county_fips"),
                year=pl.col("year").replace_strict(
                    {k: v for k, v in zip(range(3, 13), range(2010, 2020))}
                    if year_lb <= year <= 2019
                    else {k: v for k, v in zip(range(2, 7), range(2020, 2025))},
                    return_dtype=pl.Int64,
                ),
                age_grp=pl.col("age_grp")
                .replace_strict(age_grps, return_dtype=pl.String)
                .cast(age_grp_enum),
            )
            .filter(pl.col("year") == year)
            .drop("summary_lvl", "state_fips")
            .sort("county_fips", "age_grp")
        )

        lf.sink_parquet(DATA / f"pop/county_cc/county_pop_{year}.parquet")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
