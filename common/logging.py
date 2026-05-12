import logging
import os
from logging.handlers import RotatingFileHandler


class PipelineLogger:

    _configured = False

    @staticmethod
    def configure_logger(
        log_file="logs/pipeline.log",
        log_level=logging.INFO
    ):

        if PipelineLogger._configured:
            return

        os.makedirs("logs", exist_ok=True)

        formatter = logging.Formatter(
            fmt=(
                "%(asctime)s | "
                "%(levelname)s | "
                "%(name)s | "
                "%(message)s"
            ),
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        # rotating file handler
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,   # 10 MB
            backupCount=5
        )
        file_handler.setFormatter(formatter)

        root_logger = logging.getLogger()

        root_logger.setLevel(log_level)

        root_logger.addHandler(console_handler)
        root_logger.addHandler(file_handler)

        PipelineLogger._configured = True

    @staticmethod
    def get_logger(name):

        PipelineLogger.configure_logger()

        return logging.getLogger(name)
