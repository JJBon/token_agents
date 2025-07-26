import logging
import os

def configure_logging():
    log_level = os.getenv("LOG_LEVEL", "DEBUG")
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
    )
    logging.getLogger("boto3").setLevel(logging.WARNING)  # Optional noise reduction
    logging.getLogger("botocore").setLevel(logging.WARNING)

configure_logging()
