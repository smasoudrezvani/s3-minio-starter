import pandas as pd


def basic_validate(df: pd.DataFrame, required_columns: list[str]) -> None:
    """Validate presence of required columns and non-empty frame."""
    if df is None or df.empty:
        raise ValueError("DataFrame is empty.")
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")