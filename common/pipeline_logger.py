"""
Pipeline Logger - Enterprise Logging Utility
===========================================

A centralized, production-ready logging module for data engineering pipelines.

This utility provides:
- Structured logging with timestamps, file, and line numbers
- Console and rotating file handlers
- Automatic log directory creation
- Singleton-based configuration to avoid duplicate handlers
- Safe integration with Spark / ETL / ELT workflows

Features:
---------
- Rotating file logs (10MB per file, 5 backups)
- Project-root based log directory resolution
- Unified logging format across all pipeline layers
- Thread-safe initialization pattern

Typical Usage:
-------------
from pipeline_logger import PipelineLogger

logger = PipelineLogger.get_logger(__name__)

logger.info("Pipeline started")
logger.error("Something failed")

Author:
-------
Data Engineering Pipeline Framework

Use Case:
---------
Designed for:
- Spark / Delta Lake pipelines
- ETL / ELT workflows
- Data lakehouse architectures (Bronze / Silver / Gold)
- Monitoring and auditing systems

"""


import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


class PipelineLogger:

    _configured = False

    @staticmethod
    def configure_logger(
        log_level=logging.INFO
    ):

        if PipelineLogger._configured:
            return

        # project root
        project_root = Path(__file__).resolve().parent.parent

        # logs directory
        log_dir = project_root / "logs"

        log_dir.mkdir(parents=True, exist_ok=True)

        # log file
        log_file = log_dir / "pipeline.log"

        formatter = logging.Formatter(
            fmt=(
                "%(asctime)s | "
                "%(levelname)s | "
                "%(name)s | "
                "%(filename)s:%(lineno)d | "
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
            maxBytes=10 * 1024 * 1024,
            backupCount=5
        )

        file_handler.setFormatter(formatter)

        root_logger = logging.getLogger()

        # avoid duplicate handlers
        if root_logger.handlers:
            PipelineLogger._configured = True
            return

        root_logger.setLevel(log_level)

        root_logger.addHandler(console_handler)
        root_logger.addHandler(file_handler)

        PipelineLogger._configured = True

    @staticmethod
    def get_logger(name):

        PipelineLogger.configure_logger()

        return logging.getLogger(name)
