import sys
import logging
from logging import FileHandler, Formatter, StreamHandler


def configure_logger(log_filename):
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    stream_handler = logging.StreamHandler(sys.stdout)
    file_handler = logging.FileHandler(log_filename)
    formatter_instance = Formatter(fmt= "{asctime} - {levelname} - {name} - {message}", datefmt="%m/%d/%Y", style="{")
    file_handler.setFormatter(formatter_instance)
    stream_handler.setFormatter(formatter_instance)
    if not root_logger.handlers:
        root_logger.addHandler(stream_handler)
        root_logger.addHandler(file_handler)