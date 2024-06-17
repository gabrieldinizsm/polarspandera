import polars as pl
import os


def read_parquet_files(path: str) -> pl.DataFrame:
    """
    Function to read multiple parquet files in a directory.

    It starts listing all parquet files in the directory, after,
    it tries to read all of them and merging into a single DataFrame, returning it.

    Args:
        path (str): The directory path to be read.

    Returns:
        pl.DataFrame: A single DataFrame, consisting in all files merged into one structure.
    """
    try:
        files = [file for file in os.listdir(
            path) if file.endswith('.parquet')]
        dfs = [pl.scan_parquet(path + file) for file in files]
        return pl.concat(dfs).lazy()
    except Exception as e:
        print(f'Error while reading file {e}')
        raise


if __name__ == '__main__':
    df = read_parquet_files('data/')
    df = df.with_columns(
        pl.col('FL_DATE').cast(pl.Date, strict=False).alias("FLIGHT_DATE"),
        pl.col('DEP_DELAY').cast(pl.Int16).alias("DEPARTURE_DELAY"),
        (pl.col('DEP_DELAY') > 0).alias('IS_LATE'),
        pl.col('ARR_DELAY').cast(pl.Int16),
        pl.col('AIR_TIME').cast(pl.Int16),
        pl.col('DISTANCE').cast(pl.Int16),
        (pl.col('DISTANCE') / pl.col('AIR_TIME')).alias('DISTANCE_OVER_TIME'),
        pl.col('DEP_TIME').cast(pl.Float32).alias("DEPARTURE_TIME"),
        pl.col('ARR_TIME').cast(pl.Int16),
    ).collect()
    print(df.head())
