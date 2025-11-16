import json
import asyncio
from nats.aio.client import Client as NATS
from nats.js.api import StreamConfig

nc: NATS | None = None
js: any = None  # JetStream context


async def init_nats(nats_url="nats://nats:4222", retries=5, delay=2):
    """
    Initialize NATS + JetStream connection, with retries.
    Returns (nc, js) once connected.
    """
    global nc, js
    attempt = 0
    while attempt < retries:
        try:
            nc = NATS()
            await nc.connect(nats_url)
            js = nc.jetstream()
            print(f"Connected to NATS at {nats_url}")
            return nc, js
        except Exception as e:
            attempt += 1
            print(f"NATS connection attempt {attempt} failed: {e}")
            await asyncio.sleep(delay)
    raise ConnectionError(f"Failed to connect to NATS at {nats_url} after {retries} attempts")


async def ensure_stream(stream_name="DOCS", subjects=["ingest_files"]):
    try:
        global nc, js
        await js.stream_info(stream_name)
        print(f"Stream '{stream_name}' exists")
    except Exception:
        print(f"Creating stream '{stream_name}'")
        config = StreamConfig(name=stream_name, subjects=subjects, storage="file")
        await js.add_stream(config)


async def js_publish(subject: str, data: dict):
    """
    Publish a message to JetStream.
    """
    global nc, js
    if js is None:
        raise RuntimeError("JetStream not initialized. Call init_nats() first.")
    payload = json.dumps(data).encode("utf-8")
    await js.publish(subject, payload)
