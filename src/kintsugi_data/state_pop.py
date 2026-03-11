import polars as pl
import polars.selectors as cs

from .globals import DATA, DATA_UNTRACKED


def main() -> int:
    """
    Note that the race values are for "alone or in combination", not simply
    "alone" as in county population data.

    Also note that age is age itself, rather than groups.
    """
    sex = {0: "tot", 1: "male", 2: "female"}
    sex_enum = pl.Enum(sex.values())
    hispanic_origin = {0: "tot", 1: "not_hispanic", 2: "hispanic"}
    hispanic_enum = pl.Enum(hispanic_origin.values())
    race = {1: "white", 2: "black", 3: "aian", 4: "asian", 5: "nhpi"}
    race_enum = pl.Enum(race.values())

    for file in sorted(
        (DATA_UNTRACKED / "pop/state").glob("sc*.csv", case_sensitive=False)
    ):
        year = int(file.name.split("-")[1][3:])
        lf = (
            pl.scan_csv(
                file,
                null_values=["X"],
                new_columns=[
                    "summary_lvl",
                    "region",
                    "division",
                    "state_fips",
                    "state_name",
                    "sex",
                    "hispanic_origin",
                    "race",
                    "age",
                ],
                schema_overrides={
                    "summary_lvl": pl.String,
                    "region": pl.Int64,
                    "division": pl.Int64,
                    "state_fips": pl.String,
                    "state_name": pl.String,
                    "sex": pl.Int64,
                    "hispanic_origin": pl.Int64,
                    "race": pl.Int64,
                    "age": pl.Int64,
                },
            )
            .select(
                "summary_lvl",
                "region",
                "division",
                "state_fips",
                "state_name",
                "sex",
                "hispanic_origin",
                "race",
                "age",
                cs.starts_with("POPESTIMATE"),
            )
            .filter(
                pl.col("summary_lvl") == "040",
            )
            .drop("summary_lvl", "region", "division")
            .with_columns(
                sex=pl.col("sex")
                .replace_strict(sex, return_dtype=pl.String)
                .cast(sex_enum),
                hispanic_origin=pl.col("hispanic_origin")
                .replace_strict(hispanic_origin, return_dtype=pl.String)
                .cast(hispanic_enum),
                race=pl.col("race")
                .replace_strict(race, return_dtype=pl.String)
                .cast(race_enum),
            )
            .unpivot(
                index=[
                    "state_name",
                    "state_fips",
                    "sex",
                    "hispanic_origin",
                    "race",
                    "age",
                ],
                variable_name="year",
                value_name="tot_pop",
            )
            .with_columns(
                year=pl.col("year").str.slice(-4).str.to_lowercase().cast(pl.Int64)
            )
            .select(
                "state_name",
                "state_fips",
                "year",
                "age",
                "sex",
                "race",
                "hispanic_origin",
                "tot_pop",
            )
            .sort("state_fips", "year", "age", "sex", "race", "hispanic_origin")
        )

        lf.sink_parquet(DATA / f"pop/state/state_pop_{year}.parquet")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
