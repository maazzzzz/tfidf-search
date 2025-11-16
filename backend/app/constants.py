import os

STORAGE = "/app/storage"
TF_TEMP = os.path.join(STORAGE, "tf_temp")
DF_TEMP = os.path.join(STORAGE, "df_temp")
TF_INDEX = os.path.join(STORAGE, "tf_index")
DF_INDEX = os.path.join(STORAGE, "df_index")
DOCS = os.path.join(STORAGE, "docs")
VERSION_PATH = os.path.join(STORAGE, "version.json")

DURABLE = "tf_ingest_worker"
INGEST_SUBJECT = "ingest_files"