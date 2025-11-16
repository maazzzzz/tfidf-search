import os
import json
import math
import polars as pl
from .constants import *

def load_version():
    if not os.path.exists(VERSION_PATH):
        return {"version": 0, "N": 0}
    return json.load(open(VERSION_PATH))

async def search_query(term: str):
    """
    Search for a single term using precomputed N and df_count.
    Returns top 3 document IDs by TF-IDF score.
    """
    version_info = load_version()
    v = version_info["version"]
    N = version_info.get("docs_ingested", 0)

    if v == 0 or N == 0:
        return []

    tf_file = os.path.join(TF_INDEX, f"tf_{v}.parquet")
    df_file = os.path.join(DF_INDEX, f"df_{v}.parquet")

    # -----------------------------
    # Get df_count for term
    # -----------------------------
    df_scan = pl.scan_parquet(df_file).filter(pl.col("term") == term)
    df_count_series = df_scan.select("df_count").collect()
    if df_count_series.height == 0:
        return []
    df_count = df_count_series[0, 0]

    # -----------------------------
    # Compute IDF
    # -----------------------------
    idf = math.log(N / df_count)

    # -----------------------------
    # Get TF rows for term
    # -----------------------------
    tf_scan = pl.scan_parquet(tf_file).filter(pl.col("term") == term)

    # Compute score and sort top 3 in streaming mode
    top_docs = (
        tf_scan
        .with_columns((pl.col("count") * idf).alias("score"))
        .sort(["score", "doc_id"], descending=[True, False])
        .select("doc_id")
        .head(3)
        .collect()
        .to_series()
        .to_list()
    )

    return top_docs
