import logging
from logging.handlers import RotatingFileHandler

# Define color codes for different log levels
LOG_COLORS = {
    'DEBUG': '\033[32m',    # Green
    'INFO': '\033[34m',     # Blue
    'WARNING': '\033[33m',  # Yellow
    'ERROR': '\033[31m',    # Red
    'CRITICAL': '\033[35m'  # Magenta
}

# Define color reset code
LOG_COLOR_RESET = '\033[0m'


def setup_logger(name, info_only=True):
    if info_only:
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        # Custom log formatting based on log level
        class ColoredFormatter(logging.Formatter):
            def format(self, record):
                levelname = record.levelname
                record.levelname = f"{LOG_COLORS.get(levelname, '')}{levelname}{LOG_COLOR_RESET}"
                return super().format(record)

        colored_formatter = ColoredFormatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
        stream_handler.setFormatter(colored_formatter)

        logger.addHandler(stream_handler)
    else:
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

        loghdl = RotatingFileHandler(name + '.log')
        loghdl.setLevel(logging.DEBUG)
        loghdl.setFormatter(formatter)
        logger.addHandler(loghdl)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        # Custom log formatting based on log level
        class ColoredFormatter(logging.Formatter):
            def format(self, record):
                levelname = record.levelname
                record.levelname = f"{LOG_COLORS.get(levelname, '')}{levelname}{LOG_COLOR_RESET}"
                return super().format(record)

        colored_formatter = ColoredFormatter(fmt='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
        stream_handler.setFormatter(colored_formatter)

        logger.addHandler(stream_handler)

    return logger

# Set up the logger only once
log = logging.getLogger('appLog')
if not log.handlers:
    setup_logger(name='appLog')
