# File paths, constants, etc.


""""
    the point of this file is to hold the hole paths and its 
    should not be upload it to github !! 
"""""

import os
import logging

# Environment Configuration

ENV = os.getenv("ENV", "DEV")

# -----------------------------
# Directories
# -----------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed")

LOG_DIR = os.path.join(BASE_DIR, "logs")

# Ensures that the paths is existing
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

# -----------------------------
# Logging Configuration
# -----------------------------
if ENV == "DEV":
    LOG_LEVEL = logging.DEBUG
else:
    LOG_LEVEL = logging.INFO

LOG_FORMAT = "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# -----------------------------
# Data Constants
# -----------------------------
RATINGS_FILE = os.path.join(RAW_DATA_DIR, "ratings.dat")
USERS_FILE = os.path.join(RAW_DATA_DIR, "users.dat")
MOVIES_FILE = os.path.join(RAW_DATA_DIR, "movies.dat")

