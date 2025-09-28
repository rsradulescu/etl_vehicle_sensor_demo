"""
Streaming ingesiton of public dataset API integration (Hugging Face Datasets).
- https://huggingface.co/datasets/commaai/comma2k19
- https://huggingface.co/docs/datasets/en/stream
"""

from typing import Dict, Any, Iterable, Iterator, Optional
from datasets import load_dataset


def stream_records(
    dataset_name: str,
    split: str = "train",
    limit: Optional[int] = None,
    columns: Optional[list[str]] = None,
) -> Iterator[Dict[str, Any]]:
    """
    Stream samples from a public dataset without downloading it fully.
    `columns` lets you keep only a subset of keys for each record.
    """
    ds = load_dataset(dataset_name, split=split, streaming=True)
    count = 0
    for sample in ds:
        if columns:
            sample = {k: sample.get(k) for k in columns if k in sample}
        yield sample
        count += 1
        if limit is not None and count >= limit:
            break
