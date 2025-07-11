"""Logging utilities."""

import logging
import sys


class Logger:
    """Simple wrapper around :mod:`logging`.

    Attributes
    ----------
    _logger : logging.Logger | None
        Internal logger instance used for all log output.
    """

    _logger = None

    @staticmethod
    def init_logging(log_path: str) -> None:
        """Initialize logging to the given file path."""

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[logging.FileHandler(log_path), logging.StreamHandler(sys.stdout)],
        )
        Logger._logger = logging.getLogger()
        sys.stdout = Logger._StreamToLogger(Logger._logger, logging.INFO)
        sys.stderr = Logger._StreamToLogger(Logger._logger, logging.ERROR)

    @staticmethod
    def info(message: str) -> None:
        """Log an info level message."""
        if Logger._logger:
            Logger._logger.info(message)
        else:
            print(message)

    @staticmethod
    def error(message: str) -> None:
        """Log an error level message."""
        if Logger._logger:
            Logger._logger.error(message)
        else:
            print(f"ERROR: {message}", file=sys.stderr)
        sys.exit(1)

    @staticmethod
    def debug(message: str) -> None:
        """Log a debug level message."""
        if Logger._logger:
            Logger._logger.debug(message)

    @staticmethod
    def warning(message: str) -> None:
        """Log a warning level message."""
        if Logger._logger:
            Logger._logger.warning(message)
        else:
            print(f"WARNING: {message}")

    class _StreamToLogger:
        """File-like object that forwards writes to ``logging``."""

        def __init__(self, logger: logging.Logger, level: int) -> None:
            self.logger = logger
            self.level = level

        def write(self, message: str) -> None:
            if message.strip():
                self.logger.log(self.level, message.strip())

        def flush(self) -> None:
            pass
