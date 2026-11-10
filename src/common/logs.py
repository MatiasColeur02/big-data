"""Logging del pipeline: un formato único en consola, para notebooks, scripts y evidencias."""

import logging
import sys


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger("pipeline")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-5s %(message)s", "%H:%M:%S"))
        logger.addHandler(handler)
        logger.propagate = False
    logger.setLevel(level)
    return logger
