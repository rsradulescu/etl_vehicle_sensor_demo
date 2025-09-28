"""
Raw layer: write & read JSONL (newline-delimited JSON).append only
"""

from typing import Iterable, Dict, Any, Iterator
from pathlib import Path
import json


def write_jsonl(records: Iterable[Dict[str, Any]], out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return out_path


def read_jsonl(in_path: Path) -> Iterator[Dict[str, Any]]:
    with in_path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)
