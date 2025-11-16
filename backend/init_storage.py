import os
import json

STORAGE = "storage"
os.makedirs(os.path.join(STORAGE, "docs"), exist_ok=True)
os.makedirs(os.path.join(STORAGE, "tf_temp"), exist_ok=True)
os.makedirs(os.path.join(STORAGE, "df_temp"), exist_ok=True)
os.makedirs(os.path.join(STORAGE, "tf_index"), exist_ok=True)
os.makedirs(os.path.join(STORAGE, "df_index"), exist_ok=True)

version = {"version": 0}
with open(os.path.join(STORAGE, "version.json"), "w") as f:
    json.dump(version, f)
print("Initialized storage.")
