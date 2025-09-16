#!/usr/bin/env python3
"""
Transform HEPA parquet taxonomy into a slim JSONL for LLM prompts.

Input:  proc_he_codes.parquet (from your previous step)
Output: hepa_codes_slim.jsonl with fields: code, name, path
"""

import pandas as pd

# === EDIT PATHS AS NEEDED ===
INPUT_PARQUET = "proc_he_codes.parquet"
OUTPUT_JSONL = "hepa_codes_slim.jsonl"
# =============================


def make_slim_jsonl(input_path: str, output_path: str):
    # Load full parquet
    df = pd.read_parquet(input_path)

    # Pick only the slim view
    slim = df[["code", "name", "path"]].copy()

    # Save as JSON Lines (one record per line)
    slim.to_json(output_path, orient="records", lines=True, force_ascii=False)

    print(f"✅ Wrote {len(slim)} records to {output_path}")
    print("\nFirst 10 lines:\n")
    with open(output_path, "r", encoding="utf-8") as f:
        for i, line in zip(range(10), f):
            print(line.strip())


if __name__ == "__main__":
    make_slim_jsonl(INPUT_PARQUET, OUTPUT_JSONL)
