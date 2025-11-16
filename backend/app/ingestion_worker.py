import os
import json
import asyncio
import polars as pl
from nats.aio.client import Client as NATS
from nats.js.client import JetStreamContext
from .text_processing import compute_tf_df_for_file
from .constants import *
from .logger import logger

os.makedirs(TF_TEMP, exist_ok=True)
os.makedirs(DF_TEMP, exist_ok=True)


async def run_ingestion_worker():
    nc = NATS()
    await nc.connect("nats://nats:4222")  # or from env var

    js: JetStreamContext = nc.jetstream()

    sub = await js.pull_subscribe(
        subject=INGEST_SUBJECT,
        durable=DURABLE,
    )

    logger.info(f"[INGESTION WORKER] Started. Durable={DURABLE}")

    while True:
        try:
            msgs = await sub.fetch(1, timeout=5)
        except Exception:
            await asyncio.sleep(0.2)
            continue

        for msg in msgs:
            try:
                payload = json.loads(msg.data.decode())
                doc_id = payload["doc_id"]
                file_path = os.path.join(DOCS, doc_id)

                if not os.path.exists(file_path):
                    logger.info(f"[INGESTION] Missing file {doc_id}, acking.")
                    await msg.ack()
                    continue

                logger.info(f"[INGESTION] Processing {doc_id} ...")

                # compute TF + DF
                tf_rows, df_rows = compute_tf_df_for_file(file_path)

                # write parquet using Polars
                tf_path = os.path.join(TF_TEMP, f"{doc_id}.parquet")
                df_path = os.path.join(DF_TEMP, f"{doc_id}.parquet")

                # Convert list of dicts to Polars DataFrame and write
                if tf_rows:
                    tf_df = pl.DataFrame(tf_rows)
                    tf_df.write_parquet(tf_path)

                if df_rows:
                    df_df = pl.DataFrame(df_rows)
                    df_df.write_parquet(df_path)

                logger.info(f"[INGESTION] Completed {doc_id}")
                await msg.ack()

            except Exception as e:
                logger.info(f"[INGESTION] Error: {e}. Will be redelivered.")

        await asyncio.sleep(0.05)


if __name__ == "__main__":
    asyncio.run(run_ingestion_worker())