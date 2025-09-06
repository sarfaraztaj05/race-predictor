#This is for cleaning the data.
import os
from pathlib import Path
import pandas as pd
import numpy as np

DATA_ALL_CSV = Path("data/f1_final_results.csv")

OUT_CLEAN_DIR = Path("data/clean")
OUT_SPLIT_DIR = Path("data/splits")
OUT_CLEAN_DIR.mkdir(parents=True, exist_ok=True)
OUT_SPLIT_DIR.mkdir(parents=True, exist_ok=True)

# Canonical schema we’ll enforce
COLUMNS = [
    "Season","RaceName","DriverNumber","Driver","Team",
    "GridPos","FinishPos","Points","Status","QPos","QTime","QGapToPole"
]

def load_dataset():
    if DATA_ALL_CSV.exists():
        return pd.read_csv(DATA_ALL_CSV)
    raise FileNotFoundError(f"{DATA_ALL_CSV} not found. Export your combined dataset first.")

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = np.nan
    return df[COLUMNS].copy()

def to_seconds(series):
    td = pd.to_timedelta(series, errors="coerce")
    return td.dt.total_seconds()

def basic_types(df: pd.DataFrame) -> pd.DataFrame:
    # Convert time columns
    df["QTime"] = to_seconds(df["QTime"])
    df["QGapToPole"] = to_seconds(df["QGapToPole"])

    # Ensure numeric fields
    for col in ["Season","DriverNumber","GridPos","FinishPos","Points","QPos"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Strings
    for col in ["RaceName","Driver","Team","Status"]:
        df[col] = df[col].astype("string")

    return df

def drop_impossible(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["Season","Driver","Team","FinishPos"])
    return df

def fit_imputers(train_df: pd.DataFrame):
    num_cols = ["GridPos","QPos","QTime","QGapToPole","Points"]
    medians = {c: float(train_df[c].median()) if c in train_df else np.nan for c in num_cols}
    cat_cols = ["RaceName","Driver","Team","Status"]
    cat_fill = {c: "Unknown" for c in cat_cols}
    return medians, cat_fill

def apply_imputers(df: pd.DataFrame, medians: dict, cat_fill: dict) -> pd.DataFrame:
    for c, m in medians.items():
        if c in df.columns:
            df[c] = df[c].fillna(m)
    for c, val in cat_fill.items():
        if c in df.columns:
            df[c] = df[c].fillna(val)
    return df

def main():
    df = load_dataset()
    df = enforce_schema(df)
    df = basic_types(df)
    df = drop_impossible(df)

    df = df.sort_values(["Season","FinishPos","Driver"], na_position="last").reset_index(drop=True)

    # Train = 2022+2023, Test = 2024+2025
    train = df[df["Season"].isin([2022, 2023])].copy()
    test  = df[df["Season"].isin([2024, 2025])].copy()

    medians, cat_fill = fit_imputers(train)
    train = apply_imputers(train, medians, cat_fill)
    test  = apply_imputers(test,  medians, cat_fill)

    # Save cleaned master
    df.to_csv(OUT_CLEAN_DIR / "f1_clean.csv", index=False)

    # Save splits
    train.to_csv(OUT_SPLIT_DIR / "train_2022_2023.csv", index=False)
    test.to_csv(OUT_SPLIT_DIR / "test_2024_2025.csv", index=False)

    print("✅ Cleaning + split complete")
    print("Train rows:", len(train), " | Seasons:", sorted(train['Season'].unique().tolist()))
    print("Test rows :", len(test),  " | Seasons:", sorted(test['Season'].unique().tolist()))
    print("Saved to:")
    print(" -", OUT_CLEAN_DIR / "f1_clean.csv")
    print(" -", OUT_SPLIT_DIR / "train_2022_2023.csv")
    print(" -", OUT_SPLIT_DIR / "test_2024_2025.csv")

if __name__ == "__main__":
    main()