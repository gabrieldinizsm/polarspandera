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
    df = read_parquet_files('data/').collect()
    print(df.head())
