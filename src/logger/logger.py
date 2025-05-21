import logging
import os
from pathlib import Path
from typing import Optional

# Default log format
DEFAULT_LOG_FORMAT = "%(asctime)s - [%(levelname)s] - %(name)s - %(message)s - %(filename)s - %(funcName)s(%(lineno)d)"


def get_file_handler(name: str, log_path: Optional[Path] = None, log_format: str = DEFAULT_LOG_FORMAT) -> logging.FileHandler:
    """
    Create a file handler for logging to a file.

    Args:
        name (str): Logger name, used for the log file name.
        log_path (Optional[Path]): Directory for log files. Defaults to 'data/logs'.
        log_format (str): Format for log messages. Defaults to DEFAULT_LOG_FORMAT.

    Returns:
        logging.FileHandler: Configured file handler.

    Raises:
        OSError: If the log directory cannot be created or accessed.
    """
    if log_path is None:
        log_path = Path(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'data', 'logs')

    log_path = log_path.resolve()
    try:
        if not log_path.exists():
            log_path.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise OSError(f"Failed to create log directory at {log_path}: {e}")

    file_handler = logging.FileHandler(log_path / f"{name}.log", mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(log_format))
    return file_handler


def get_stream_handler(log_format: str = DEFAULT_LOG_FORMAT) -> logging.StreamHandler:
    """
    Create a stream handler for logging to the console.

    Args:
        log_format (str): Format for log messages. Defaults to DEFAULT_LOG_FORMAT.

    Returns:
        logging.StreamHandler: Configured stream handler.
    """
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter(log_format))
    return stream_handler


def get_logger(
    name: str,
    level: str = 'INFO',
    log_path: Optional[Path] = None,
    file_log_level: str = 'INFO',
    stream_log_level: str = 'INFO',
    log_format: str = DEFAULT_LOG_FORMAT
) -> logging.Logger:
    """
    Create a logger with file and stream handlers.

    Args:
        name (str): Logger name.
        level (str): Overall logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'). Defaults to 'INFO'.
        log_path (Optional[Path]): Directory for log files. Defaults to 'data/logs'.
        file_log_level (str): Log level for the file handler. Defaults to 'INFO'.
        stream_log_level (str): Log level for the stream handler. Defaults to 'INFO'.
        log_format (str): Format for log messages. Defaults to DEFAULT_LOG_FORMAT.

    Returns:
        logging.Logger: Configured logger.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:  # Prevent duplicate handlers
        logger.setLevel(getattr(logging, level.upper(), logging.INFO))
        
        # Configure file handler
        file_handler = get_file_handler(name, log_path, log_format)
        file_handler.setLevel(getattr(logging, file_log_level.upper(), logging.INFO))
        logger.addHandler(file_handler)

        # Configure stream handler
        stream_handler = get_stream_handler(log_format)
        stream_handler.setLevel(getattr(logging, stream_log_level.upper(), logging.INFO))
        logger.addHandler(stream_handler)

    return logger