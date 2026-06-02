import re


def to_snake_case(name: str) -> str:
    """Convert a single column name to snake_case.
    Strips punctuation, lowercases, replaces spaces with underscores.
    """
    name = re.sub(r"[^\w\s]", "", name)        # drop punctuation like '/', '-'
    return re.sub(r"\s+", "_", name.strip().casefold())


def rename_columns_to_snake_case(df):
    """Apply snake_case to every column in a DataFrame."""
    return df.toDF(*[to_snake_case(c) for c in df.columns])