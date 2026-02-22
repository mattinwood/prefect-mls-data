import logging
import sys


class ColoredFormatter(logging.Formatter):
    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[94m',    # Cyan
        'INFO': '\033[92m',     # Green
        'WARNING': '\033[93m',  # Yellow
        'ERROR': '\033[91m',    # Red
        'CRITICAL': '\033[95m', # Magenta
    }
    RESET = '\033[0m'

    def format(self, record):
        # Pad the levelname to 8 characters (length of 'CRITICAL') for alignment
        original_levelname = record.levelname
        record.levelname = record.levelname.ljust(8)

        # Get the formatted message with padded levelname
        log_message = super().format(record)

        # Restore original levelname
        record.levelname = original_levelname

        # Add color based on log level
        color = self.COLORS.get(record.levelname, '')
        return f"{color}{log_message}{self.RESET}"


def get_logger(name='customlogger', level=logging.DEBUG):
    """
    Get a logger with colored output and custom datetime format.

    Args:
        name (str, optional): Logger name. Defaults to calling module name.
        level (int, optional): Logging level. Defaults to logging.INFO.

    Returns:
        logging.Logger: Configured logger instance.
    """
    # Use calling module name if no name provided
    if name is None:
        import inspect
        frame = inspect.currentframe().f_back
        name = frame.f_globals.get('__name__', 'root')

    # Create logger
    logger = logging.getLogger(name)

    # Avoid adding handlers multiple times
    if logger.handlers:
        return logger

    # Set level
    logger.setLevel(level)

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)

    # Create formatter
    # Format: 2025-09-10 13:30:36.145 - INFO - Your message here
    formatter = ColoredFormatter(
        fmt='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Add formatter to handler
    console_handler.setFormatter(formatter)

    # Add handler to logger
    logger.addHandler(console_handler)

    return logger


if __name__ == "__main__":
    # Demo the logger
    logger = get_logger()

    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")
