# logger.py
import logging

logger = logging.getLogger("tfidf_app")
logger.setLevel(logging.INFO)

# StreamHandler writes to stdout (captured by Docker)
handler = logging.StreamHandler()
formatter = logging.Formatter("[%(levelname)s] %(asctime)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)