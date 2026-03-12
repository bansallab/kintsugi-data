import polars as pl

from .globals import DATA, DATA_PRE


def main() -> int:
    """
    Convert zip-to-county and county-to-zip crosswalk
    files to provide both options.
    """
    convert_zip_county()
    convert_county_zip()

    return 0


def convert_zip_county() -> None:
    for file in sorted((DATA_PRE / "crosswalk/zip_to_county").glob("*")):
        year = int(file.stem[-4:])
        df = (
            pl.read_excel(
                file,
            )
            .rename(lambda col: col.lower())
            .rename(
                {"zip": "zip_code", "county": "county_fips", "res_ratio": "res_ratio"}
            )
            .cast(
                {
                    "zip_code": pl.String,
                    "county_fips": pl.String,
                    "res_ratio": pl.Float64,
                }
            )
            .filter(
                pl.col("county_fips")
                .str.slice(0, 2)
                .is_between(pl.lit("01"), pl.lit("56"))
            )
            .select("zip_code", "county_fips", "res_ratio")
            .sort("zip_code", "county_fips")
        )

        df.write_parquet(DATA / f"crosswalk/zip_to_county/zip_to_county_{year}.parquet")


def convert_county_zip() -> None:
    for file in sorted((DATA_PRE / "crosswalk/county_to_zip").glob("*")):
        year = int(file.stem[-4:])
        df = (
            pl.read_excel(
                file,
            )
            .rename(lambda col: col.lower())
            .rename(
                {"zip": "zip_code", "county": "county_fips", "res_ratio": "res_ratio"}
            )
            .cast(
                {
                    "zip_code": pl.String,
                    "county_fips": pl.String,
                    "res_ratio": pl.Float64,
                }
            )
            .filter(
                pl.col("county_fips")
                .str.slice(0, 2)
                .is_between(pl.lit("01"), pl.lit("56"))
            )
            .select("zip_code", "county_fips", "res_ratio")
            .sort("county_fips", "zip_code")
        )

        df.write_parquet(DATA / f"crosswalk/county_to_zip/county_to_zip_{year}.parquet")


if __name__ == "__main__":
    raise SystemExit(main())
