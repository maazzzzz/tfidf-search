import os
import json
from datetime import datetime
import polars as pl
import time
from .logger import logger
from .constants import *

def load_version():
    if not os.path.exists(VERSION_PATH):
        return {"version": 0, "N": 0}  # include N by default
    return json.load(open(VERSION_PATH))

def save_version(obj):
    tmp = VERSION_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f)
    os.replace(tmp, VERSION_PATH)

def merge_once():
    version = load_version()
    new_version = version["version"] + 1
    logger.info(f"[MERGE] Starting version {new_version}")
    merged_any = False

    # Merge TF
    tf_temp_files = [os.path.join(TF_TEMP, f) for f in os.listdir(TF_TEMP) if f.endswith(".parquet")]
    df_temp_files = [os.path.join(DF_TEMP, f) for f in os.listdir(DF_TEMP) if f.endswith(".parquet")]
    if len(tf_temp_files) == 0 and len(df_temp_files) == 0:
        logger.info("[MERGE] No TF/DF temp files to merge.")
        return False

    prev_tf_file = os.path.join(TF_INDEX, f"tf_{version['version']}.parquet")
    tf_sources = []

    if os.path.exists(prev_tf_file):
        tf_sources.append(pl.scan_parquet(prev_tf_file))
    for f in tf_temp_files:
        tf_sources.append(pl.scan_parquet(f))

    tf_file_out = None
    if tf_sources:
        tf_file_out = os.path.join(TF_INDEX, f"tf_{new_version}.parquet")
        (
            pl.concat(tf_sources)
            .group_by(["doc_id", "term"])
            .agg(pl.col("count").sum())
            .sink_parquet(tf_file_out)
        )
        merged_any = True
        for f in tf_temp_files:
            os.remove(f)

    # Merge DF
    prev_df_file = os.path.join(DF_INDEX, f"df_{version['version']}.parquet")
    df_sources = []

    if os.path.exists(prev_df_file):
        df_sources.append(pl.scan_parquet(prev_df_file))
    for f in df_temp_files:
        df_sources.append(pl.scan_parquet(f))

    df_file_out = None
    if df_sources:
        df_file_out = os.path.join(DF_INDEX, f"df_{new_version}.parquet")
        df_merged = (
            pl.concat(df_sources)
            .group_by("term")
            .agg(pl.col("df_count").sum())
        )
        df_merged.collect().write_parquet(df_file_out)
        merged_any = True
        for f in df_temp_files:
            os.remove(f)

    # Precompute N (total distinct docs)
    N = 0
    if tf_file_out and os.path.exists(tf_file_out):
        N = pl.scan_parquet(tf_file_out).select("doc_id").unique().count().collect()[0, 0]
        logger.info(f"[MERGE] Precomputed total distinct docs N = {N}")

    # Save version info
    if merged_any:
        save_version({
            "version": new_version,
            "timestamp": datetime.utcnow().isoformat(),
            "docs_ingested": N
        })

    logger.info(f"[MERGE] Completed version {new_version}")
    return merged_any

def merge_worker_loop(interval_seconds: int = 60):
    logger.info("[MERGE WORKER] Started merge worker loop")
    while True:
        try:
            merge_once()
        except Exception as e:
            logger.error(f"[MERGE WORKER] Error: {e}")
        time.sleep(interval_seconds)

if __name__ == "__main__":
    interval = int(os.getenv("MERGE_INTERVAL", 60))
    merge_worker_loop(interval_seconds=interval)
