import polars as pl
import os
import sys
sys.path.append(os.getcwd())


def read_parquet_files(path: str) -> pl.DataFrame:
    """
    Read multiple parquet files in a directory.

    Starts by listing all parquet files in the directory, then
    it tries to read all of them merging into a single DataFrame.

    Args:
        path (str): The directory path to be read.

    Returns:
        pl.DataFrame: A single DataFrame, consisting in all files merged.
    """
    try:
        files = [file for file in os.listdir(
            path) if file.endswith('.parquet')]
        dfs = [pl.scan_parquet(path + file) for file in files]
        return pl.concat(dfs).lazy()
    except FileNotFoundError as e:
        print(f'File not found: {e}')
    
    except IOError as e:
        print(f'I/O Error: {e}')

    except Exception as e:
        print(f'Unexpected Error while reading file: {e}')
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
    )

    from src.contracts.contract import FlightSchema
    df = FlightSchema.schema.validate(df).collect()
    print(df.head())
