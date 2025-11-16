import polars as pl
from typing import List, Dict, Tuple

def compute_tf_df_for_file(path: str) -> Tuple[List[Dict], List[Dict]]:
    """
    Compute term frequency (TF) and document frequency (DF) for a single document.
    Uses Polars for fast, memory-efficient text processing.
    
    Args:
        path: Path to the document file
    
    Returns:
        - tf_rows: List of {doc_id, term, count} dictionaries
        - df_rows: List of {term, df_count} where df_count=1 (single doc)
    """
    # Read file as single string
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        text = f.read()
    
    # Extract document ID from filename
    doc_id = path.split("/")[-1]
    
    # Create dataframe with the text
    df = pl.DataFrame({"text": [text]})
    
    # Extract all tokens (words), explode to individual rows, and count
    token_counts = (
        df
        .select(
            pl.col("text")
            .str.to_lowercase()
            .str.extract_all(r'\b\w+\b')  # Extract all word tokens
            .alias("tokens")
        )
        .explode("tokens")  # One row per token
        .group_by("tokens")
        .agg(pl.len().alias("count"))  # Count occurrences
        .filter(pl.col("tokens").is_not_null())  # Remove nulls
        .sort("count", descending=True)  # Optional: sort by frequency
    )
    
    # Convert to output format
    tf_rows = [
        {"doc_id": doc_id, "term": row["tokens"], "count": row["count"]}
        for row in token_counts.iter_rows(named=True)
    ]
    
    df_rows = [
        {"term": row["tokens"], "df_count": 1}
        for row in token_counts.iter_rows(named=True)
    ]
    
    return tf_rows, df_rows