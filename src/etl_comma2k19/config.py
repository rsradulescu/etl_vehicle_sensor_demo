from dataclasses import dataclass
from pathlib import Path
import os


try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

@dataclass
class Config:
    # Source
    DATASET_NAME: str = os.getenv("DATASET_NAME", "commaai/comma2k19")
    SPLIT: str = os.getenv("SPLIT", "train")

    # Extraction
    EXTRACT_LIMIT: int = int(os.getenv("EXTRACT_LIMIT", "2000"))

    # Layers
    RAW_PATH: Path = Path(os.getenv("RAW_PATH", "data/raw/comma2k19_raw.jsonl"))
    PROCESSED_DIR: Path = Path(os.getenv("PROCESSED_DIR", "data/processed"))

init_config = Config()


def ensure_dirs():
    """Create project data directories if they don't exist."""
    init_config.RAW_PATH.parent.mkdir(parents=True, exist_ok=True)
    #init_config.SILVER_DIR.mkdir(parents=True, exist_ok=True)
    init_config.PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
