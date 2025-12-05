from pathlib import Path

SRC_DIR = Path(__file__).resolve().parent
ROOT_DIR = SRC_DIR.parent

DATA_DIR = ROOT_DIR / "data"
DATAFRAME_ASSISTANT_SAMPLE_CSV_PATH = DATA_DIR / "health_lifestyle_dataset.csv"

MODELS_DIR = ROOT_DIR / "models"
DECISION_TREE_DIR = MODELS_DIR / "decision_tree"

SQLITE_CHECKPOINTER_DB_PATH = ROOT_DIR / "state_db/conversation.db"

OLLAMA_API_URL = "http://localhost:11434"

OLLAMA_MODEL = "gpt-oss:20b"  # pick model of your choice, e.g. 'gpt-oss:20b', 'llama3.1:8b', etc.
TEMPERATURE = 0.7
