import logging
import os
from pathlib import Path
from typing import Optional

_log_format = "%(asctime)s - [%(levelname)s] - %(name)s - %(message)s - %(filename)s - %(funcName)s(%(lineno)d)"


def get_file_handler(name: str, log_path: Optional[Path] = None) -> logging.FileHandler:
    """
    Create a file handler for logging to a file.

    Args:
        name (str): Logger name, used for the log file name.
        log_path (Optional[Path]): Directory for log files. Defaults to 'data/logs'.

    Returns:
        logging.FileHandler: Configured file handler.
    """
    if log_path is None:
        log_path = Path(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'data', 'logs')
    
    log_path = log_path.resolve()
    if not log_path.exists():
        log_path.mkdir(parents=True)

    file_handler = logging.FileHandler(log_path / f"{name}.log", mode='a')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(_log_format))
    return file_handler


def get_stream_handler() -> logging.StreamHandler:
    """
    Create a stream handler for logging to the console.

    Returns:
        logging.StreamHandler: Configured stream handler.
    """
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter(_log_format))
    return stream_handler


def get_logger(name: str, level: str = 'INFO', log_path: Optional[Path] = None) -> logging.Logger:
    """
    Create a logger with file and stream handlers.

    Args:
        name (str): Logger name.
        level (str): Logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'). Defaults to 'INFO'.
        log_path (Optional[Path]): Directory for log files. Defaults to 'data/logs'.

    Returns:
        logging.Logger: Configured logger.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:  # Prevent duplicate handlers
        logger.setLevel(getattr(logging, level.upper(), logging.INFO))
        logger.addHandler(get_file_handler(name, log_path))
        logger.addHandler(get_stream_handler())
    return logger
