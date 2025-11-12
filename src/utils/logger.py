# utils/logger.py

# Custom logging setup
# Handles all logging setup for the project
# Examples: Configure log formatting, levels, handlers

import os, logging
from logging.handlers import RotatingFileHandler 
from utils.config import LOG_DIR, LOG_LEVEL

def get_logger(name: str) -> logging.Logger:
    """
    Creates and returns a logger with both console and file handlers.
    """
    # Creating Logger 1
    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)
    
    # Checking if its there is handler or not 2
    if logger.hasHandlers():
        return logger
    
    # 3 Formatting 
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 4 Consle handler
    consle_handler = logging.StreamHandler()
    consle_handler.setFormatter(formatter)
    logger.addHandler(consle_handler)

    # 5 File Handler
    os.makedirs(LOG_DIR, exist_ok=True)
    file_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, f'{name}.log'),
        maxBytes=5*1024*1024,  # 5 MB
        backupCount=3
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger