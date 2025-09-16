#!/usr/bin/env python3
"""
Flatten HEPA Excel taxonomy (Level 1 + Level 2) into a clean dictionary parquet.

Just open this file in PyCharm and run it.
It will produce a Parquet file and print the first 20 rows.
"""

import re
import pandas as pd

# === EDIT THESE PATHS FOR YOUR ENVIRONMENT ===
INPUT_XLSX = "proc_he_codes.xlsx"
OUTPUT_PARQUET = "proc_he_codes.parquet"
# =============================================


def normalize_cell(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip().replace("\n", " ").replace("\t", " ")
    return re.sub(r"\s+", " ", s)


def extract_level1_map(df_l1: pd.DataFrame) -> dict:
    """Map Level-1 codes (A, B, C…) to their names."""
    code_col, name_col = df_l1.columns[0], df_l1.columns[1]
    l1_map = {}
    for _, row in df_l1.iterrows():
        code = normalize_cell(row[code_col])
        name = normalize_cell(row[name_col])
        if code and len(code) == 1 and code.isalpha():
            l1_map[code] = name
    return l1_map


def tidy_letter_sheet(df: pd.DataFrame, sheet_name: str, l1_map: dict) -> pd.DataFrame:
    parent = sheet_name.strip()
    if len(parent) != 1 or not parent.isalpha():
        return pd.DataFrame()

    code_col = df.columns[0]
    desc_col = df.columns[1]
    comment_cols = df.columns[2:]

    # Normalize
    for c in df.columns:
        df[c] = df[c].map(normalize_cell)

    # Keep only rows where code looks like "EA", "EB", ...
    mask = df[code_col].str.match(rf"{parent}[A-Z]{{1,3}}", na=False)
    df = df.loc[mask].copy()

    # Merge comments if any
    if comment_cols.any():
        df["comments"] = (
            df[comment_cols]
            .apply(lambda r: " | ".join([x for x in r if x]), axis=1)
            .replace({"": pd.NA})
        )
    else:
        df["comments"] = pd.NA

    out = pd.DataFrame(
        {
            "level1_code": parent,
            "level1_name": l1_map.get(parent, pd.NA),
            "level2_code": df[code_col],
            "level2_name": df[desc_col],
            "comments": df["comments"],
        }
    )
    return out


def build_dictionary(xlsx_path: str, output_path: str) -> pd.DataFrame:
    sheets = pd.read_excel(xlsx_path, sheet_name=None, dtype=str)

    # Build Level 1 mapping
    l1_sheet = sheets.get("Level 1")
    if l1_sheet is None:
        raise ValueError("No 'Level 1' sheet found in the Excel file.")
    l1_map = extract_level1_map(l1_sheet)

    # Process lettered sheets
    pieces = []
    for name, df in sheets.items():
        if name == "Level 1":
            continue
        if len(name) == 1 and name.isalpha():
            pieces.append(tidy_letter_sheet(df, name, l1_map))

    df_all = pd.concat(pieces, ignore_index=True)

    # Add convenience columns
    df_all["level"] = 2
    df_all["code"] = df_all["level2_code"]
    df_all["name"] = df_all["level2_name"]
    df_all["path"] = (
        df_all["level1_code"]
        + " "
        + df_all["level1_name"].fillna("").str.strip()
        + " > "
        + df_all["level2_code"]
        + " "
        + df_all["level2_name"]
    ).str.replace(r"\s+", " ", regex=True)

    # Stable surrogate key
    df_all = df_all.sort_values(["level1_code", "level2_code"], kind="stable").reset_index(drop=True)
    df_all.insert(0, "hepa_id", df_all.index + 1)

    # Write parquet
    df_all.to_parquet(output_path, engine="pyarrow", compression="snappy")
    return df_all


if __name__ == "__main__":
    df = build_dictionary(INPUT_XLSX, OUTPUT_PARQUET)
    print(f"✅ Wrote {len(df)} Level-2 HEPA codes to {OUTPUT_PARQUET}\n")
    print("First 20 rows:\n")
    print(df.head(20).to_string(index=False))
