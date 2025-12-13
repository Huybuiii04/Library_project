import os
import logging
from pathlib import Path

# Define BASE_DIR - root project directory
BASE_DIR = Path(__file__).resolve().parent.parent.parent

CHECKPOINT_FILE = "/tmp/openlibrary_checkpoint.txt"

def load_checkpoint():
    """Load the last successfully processed offset from checkpoint file."""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                offset = int(f.read().strip())
                logging.info(f"Checkpoint loaded: offset {offset}")
                return offset
        except Exception as e:
            logging.warning(f"Failed to load checkpoint: {e}")
            return 0
    return 0

def save_checkpoint(offset):
    """Save the current offset to checkpoint file."""
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            f.write(str(offset))
        logging.info(f"Checkpoint saved: offset {offset}")
    except Exception as e:
        logging.error(f"Failed to save checkpoint: {e}")
