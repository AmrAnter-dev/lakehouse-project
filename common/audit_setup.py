"""
Monitoring Layer - Audit Table Setup (Lakehouse Architecture)
=============================================================

This script initializes the core auditing infrastructure for the Lakehouse
data platform by creating the `load_audit` Delta table inside the monitoring database.

Overview:
---------
The audit layer is responsible for tracking and monitoring all ETL pipeline
executions across Bronze, Silver, and Gold layers.

It captures detailed metadata about each pipeline run for observability,
debugging, and operational monitoring.

Key Responsibilities:
---------------------
- Initialize Spark session with Delta Lake support
- Ensure monitoring database is available
- Create audit table (idempotent operation)
- Define structured schema for ETL tracking
- Support full pipeline observability

Audit Table Structure:
----------------------
monitoring_db.load_audit

Columns:
    - batch_id        → Unique identifier per pipeline run
    - table_name      → Target table being processed
    - load_start_time → Pipeline start timestamp
    - load_end_time   → Pipeline completion timestamp
    - load_status     → SUCCESS / FAILED
    - row_loaded      → Number of rows processed
    - error_message   → Error details (if any)

Architecture Role:
------------------
This table is part of the **Monitoring Layer** in the Medallion Architecture:

    Bronze  → Raw ingestion tracking
    Silver  → Data quality tracking
    Gold    → Business metrics tracking
    Monitoring → Pipeline observability & audit logs

Design Principles:
------------------
- Idempotent table creation (safe to rerun)
- Centralized ETL observability
- Structured logging for downstream analysis
- Supports failure tracking and debugging
- Delta Lake for ACID compliance

Technologies Used:
------------------
- Apache Spark (PySpark)
- Delta Lake
- Hive Metastore
- Custom Pipeline Logger

Execution Flow:
---------------
1. Start Spark session
2. Switch to monitoring database
3. Create audit table if not exists
4. Validate table creation
5. Stop Spark session safely
6. Log execution time

Use Case:
--------
This audit table is used by all ETL pipelines to record:
- Successful loads
- Failed executions
- Row counts per batch
- Execution timing metrics

Author:
------
Data Engineering Lakehouse Framework

Notes:
------
This is a foundational component for enterprise-grade pipeline observability.
"""
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pipeline_logger import PipelineLogger
import time

logger = PipelineLogger.get_logger(__name__)

start_time = time.time()

try:
    logger.info("Starting Spark session")
    
    builder = (
            SparkSession.builder
            .appName("AuditSetup")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", "/mnt/c/spark-warehouse")
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension"
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .enableHiveSupport()
        )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    logger.info("Spark session created successfully")
    
    logger.info(" initiating  monitoring_db.load_audit table")
    
    spark.sql("USE monitoring_db")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS monitoring_db.load_audit (
        batch_id STRING,
        table_name STRING,
        load_start_time TIMESTAMP,
        load_end_time TIMESTAMP,
        load_status STRING,
        row_loaded BIGINT,
        error_message STRING
    )
    USING DELTA
    """)
    logger.info("creating monitoring_db.load_audit table sucessfully")
    
    spark.sql("SHOW TABLES IN monitoring_db").show(truncate=False)
    
    
except Exception as e:
    
    logger.exception("table creation failed")

    raise

finally:

    try:

        spark.stop()

        logger.info("Spark session stopped")

    except:

        pass

    execution_time = time.time() - start_time

    logger.info(f"Total execution time: {execution_time:.2f} seconds")
