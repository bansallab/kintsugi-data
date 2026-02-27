import polars as pl

from .globals import DATA, DATA_PRE


def main() -> int:
    """
    Note that this file only has counties that are part of groups (with < 20k pop.). Need to source
    the other counties from somewhere else.
    """
    df = (
        pl.read_excel(
            DATA_PRE / "county_grouping.xlsx",
            has_header=False,
        )
        .rename({"column_2": "county_fips"})
        .select("county_fips")
        .with_columns(
            county_fips=pl.col("county_fips").str.split("|"),
            county_group=pl.col("county_fips").str.split("|"),
        )
        .with_row_index()
    )
    df_out = (
        df.explode("county_fips")
        .join(df.explode("county_fips"), on="index", suffix="_neighbor")
        .drop("county_group_neighbor")
        .filter(pl.col("county_fips") != pl.col("county_fips_neighbor"))
        .select("index", "county_fips", "county_fips_neighbor", "county_group")
        .sort("index", "county_fips", "county_fips_neighbor")
    )
    assert (
        df_out.with_columns(group_size=pl.col("county_group").list.len())
        .with_columns(
            index_ct=pl.len().over("index"),
            # NOTE: number of directed edges = n * (n - 1)
            edges=pl.col("group_size") * (pl.col("group_size") - 1),
        )
        .select((pl.col("index_ct") == pl.col("edges")).all())
        .item()
        is True
    )

    df_out = df_out.drop("index")
    df_out.write_parquet(DATA / "county_groups.parquet")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
