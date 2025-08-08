import os
import logging
import json
from logging.handlers import RotatingFileHandler
from datetime import datetime
from typing import Optional

try:
    from colorlog import ColoredFormatter
except ImportError:
    ColoredFormatter = None  # Optional if colorlog is not installed


LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "app.log")
MAX_LOG_SIZE = 5 * 1024 * 1024  # 5 MB
BACKUP_COUNT = 5

# ENV FLAGS (You can set these in deployment)
USE_JSON_LOGS = os.getenv("USE_JSON_LOGS", "false").lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "module": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_record)


def get_logger(name: str, use_json: Optional[bool] = None) -> logging.Logger:
    if use_json is None:
        use_json = USE_JSON_LOGS

    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

    if logger.hasHandlers():
        return logger  # Avoid duplicate handlers

    # Rotating File Handler
    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=MAX_LOG_SIZE, backupCount=BACKUP_COUNT)
    file_handler.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    if use_json:
        file_handler.setFormatter(JsonFormatter())
    else:
        file_formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s", "%Y-%m-%d %H:%M:%S"
        )
        file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    if not use_json and ColoredFormatter:
        console_formatter = ColoredFormatter(
            "%(log_color)s[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'bold_red',
            }
        )
        console_handler.setFormatter(console_formatter)
    else:
        console_handler.setFormatter(logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s", "%Y-%m-%d %H:%M:%S"
        ))
    logger.addHandler(console_handler)

    return logger


# ✅ Add this at the bottom to enable global import across project
logger = get_logger("my-ai-data-agent")
