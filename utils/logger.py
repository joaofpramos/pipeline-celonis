import os
import logging
from datetime import datetime

def get_logger(script_name):
    # Create the base logs directory if it doesn't exist
    base_logs_dir = "logs"
    os.makedirs(base_logs_dir, exist_ok=True)

    # Create a directory for the specific script
    script_logs_dir = os.path.join(base_logs_dir, script_name)
    os.makedirs(script_logs_dir, exist_ok=True)

    # Generate a log file with a timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file = os.path.join(script_logs_dir, f"{timestamp}.log")

    # Configure the logger
    logger = logging.getLogger(script_name)
    logger.setLevel(logging.DEBUG)

    # Create file handler to write logs to the file
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)

    # Create console handler (optional, to also show logs on the console)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Set log format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add handlers to the logger
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
