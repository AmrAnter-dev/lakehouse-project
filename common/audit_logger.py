"""
Audit Logger - Enterprise ETL Monitoring Service
================================================

This module provides a centralized audit logging service for tracking
ETL pipeline executions across a Lakehouse architecture.

It records both successful and failed pipeline runs into a Delta
audit table for observability, debugging, and operational monitoring.

Overview:
---------
The AuditLogger ensures full traceability of all data pipeline executions
by writing structured logs into a monitoring Delta table.

It is designed to be used across all layers:
- Bronze ingestion pipelines
- Silver transformation pipelines
- Gold aggregation pipelines

Key Responsibilities:
---------------------
- Log successful ETL executions
- Log failed ETL executions with error details
- Persist logs into Delta Lake audit table
- Maintain consistent schema across all pipeline layers
- Enable pipeline observability and monitoring

Audit Table Schema:
-------------------
monitoring_db.load_audit

Fields:
    - batch_id        → Unique pipeline execution identifier
    - table_name      → Target table processed
    - start_time      → Pipeline start timestamp
    - end_time        → Pipeline end timestamp
    - status          → SUCCESS / FAILED
    - row_count       → Number of rows processed
    - error_message   → Error details (if any)

Design Principles:
------------------
- Centralized logging for all ETL pipelines
- Append-only audit history (Delta Lake)
- Schema enforcement for consistency
- Fault-tolerant logging (does not stop pipeline execution)
- Scalable across distributed Spark workloads

Usage:
-----
from audit_logger import AuditLogger

auditlogger = AuditLogger(spark)

auditlogger.success(...)
auditlogger.failed(...)

Architecture Role:
------------------
This service is part of the **Monitoring Layer** in a Medallion Architecture:

    Bronze  → Raw ingestion tracking
    Silver  → Transformation tracking
    Gold    → Business aggregation tracking
    Monitoring → ETL observability & audit logs

Author:
------
Data Engineering Lakehouse Framework

Notes:
------
This component is critical for production-grade pipeline observability
and is designed for distributed Spark environments.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import lit, current_timestamp
from datetime import datetime


class AuditLogger:
    """
    Enterprise-grade audit logging service for ETL pipelines.
    """

    def __init__(self, spark: SparkSession, table_name: str = "monitoring_db.load_audit"):
        self.spark = spark
        self.table_name = table_name

    def _write_log(self, record: dict):
        """
        Internal method to write log to Delta table.
        """
        schema = StructType([
                            StructField("batch_id", StringType(), True),
                            StructField("table_name", StringType(), True),
                            StructField("start_time", TimestampType(), True),
                            StructField("end_time", TimestampType(), True),
                            StructField("status", StringType(), True),
                            StructField("row_count", LongType(), True),
                            StructField("error_message", StringType(), True)
                        ])
        df = self.spark.createDataFrame([record], schema=schema)

        df.write \
            .format("delta") \
            .mode("append") \
            .insertInto(self.table_name)

    def success(self, batch_id, table_name, start_time, end_time, row_count):
        """
        Log successful load.
        """

        record = {
            "batch_id": batch_id,
            "table_name": table_name,
            "load_start_time": start_time,
            "load_end_time": end_time,
            "load_status": "SUCCESS",
            "row_loaded": row_count,
            "error_message": None
        }

        self._write_log(record)

    def failed(self, batch_id, table_name, start_time, end_time, error_message):
        """
        Log failed load.
        """

        record = {
            "batch_id": batch_id,
            "table_name": table_name,
            "load_start_time": start_time,
            "load_end_time": end_time,
            "load_status": "FAILED",
            "row_loaded": 0,
            "error_message": str(error_message)
        }

        self._write_log(record)
