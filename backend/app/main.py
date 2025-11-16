import os
import uuid
from fastapi import FastAPI, UploadFile, File, HTTPException,BackgroundTasks
import aiofiles
from .jetstream_client import *
from .search import search_query
from .logger import logger
from .constants import *
# Storage directories
os.makedirs(DOCS, exist_ok=True)

app = FastAPI(title="TF-IDF Search (JetStream + Polars)")

@app.on_event("startup")
async def startup():
    
    # Initialize NATS & JetStream
    global nc, js 
    nc, js= await init_nats()
    await ensure_stream()
    
    logger.info("Startup complete.")

async def write_and_enqueue(file: UploadFile, dst: str, doc_id: str):
    # write the file to disk in chunks
    async with aiofiles.open(dst, "wb") as f:
        while chunk := await file.read(1024 * 1024):
            await f.write(chunk)
    logger.info(f"Wrote file {doc_id} to disk. Sending to ingestion worker.")
    # enqueue file for vectorization
    await js_publish(INGEST_SUBJECT, {"doc_id": doc_id})

@app.post("/ingest")
async def ingest(file: UploadFile = File(...), background_tasks: BackgroundTasks = None):
    if not file.filename.lower().endswith(".txt"):
        raise HTTPException(status_code=400, detail="Only .txt files allowed")

    # uid = uuid.uuid4().hex
    safe_name = os.path.basename(file.filename)
    dst = os.path.join(DOCS, safe_name)

    # write to disk + vectorization in background
    background_tasks.add_task(write_and_enqueue, file, dst, safe_name)

    return {"status": "queued", "doc_id": safe_name}

@app.get("/search")
async def search(query: str):
    if not query:
        raise HTTPException(status_code=400, detail="Empty query")
    results = await search_query(query)
    return {"query": query, "results": results}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
