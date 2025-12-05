import logging

def suppress_common_packages():
    """Suppress INFO logs from common packages to reduce clutter"""
    packages = ('httpx', )
    for p in packages:
        p_logger = logging.getLogger(p)
        p_logger.setLevel(logging.WARNING)  # setting warning level to various packages to avoid additional logs


suppress_common_packages()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("dataframe-chatbot")
