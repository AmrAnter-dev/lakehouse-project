"""
Lakehouse Infrastructure Setup Script (Medallion Architecture)
==============================================================

This script initializes the foundational infrastructure for a Lakehouse
data platform using Apache Spark and Delta Lake.

It is responsible for creating the core databases required for the
Medallion Architecture (Bronze, Silver, Gold) along with a Monitoring layer.

Overview:
---------
This script sets up the physical and logical structure of the data platform
before any ingestion or transformation pipelines are executed.

Key Responsibilities:
---------------------
- Initialize Spark session with Delta Lake support
- Configure Hive metastore integration
- Create core Lakehouse databases:
    - monitoring_db → ETL monitoring and audit tracking
    - bronze_db     → raw ingested data (Bronze layer)
    - silver_db     → cleaned and validated data (Silver layer)
    - gold_db       → analytical star schema (Gold layer)
- Assign storage locations for each database
- Ensure idempotent setup using IF NOT EXISTS
- Log full execution lifecycle using PipelineLogger

Architecture:
-------------
This script represents the **foundation layer** of a Medallion Architecture:

    Bronze  → Raw ingestion layer
    Silver  → Cleaned / transformed data layer
    Gold    → Business / analytical layer
    Monitoring → Operational logging & auditing

Technologies Used:
------------------
- Apache Spark (PySpark)
- Delta Lake
- Hive Metastore (enableHiveSupport)
- Custom Pipeline Logger

Execution Flow:
---------------
1. Start Spark session with Delta + Hive support
2. Initialize logging system
3. Define Lakehouse databases and storage locations
4. Create databases if they do not exist
5. Validate setup by listing databases
6. Stop Spark session safely
7. Log execution time

Design Principles:
------------------
- Idempotent execution (safe to rerun)
- Environment-independent (local / WSL / cloud-ready)
- Separation of concerns (infra vs ingestion vs transformation)
- Production-ready logging and observability

Output:
------
- Spark databases:
    monitoring_db
    bronze_db
    silver_db
    gold_db

- Physical storage locations assigned per database
- Execution logs stored via PipelineLogger

Author:
------
Data Engineering Lakehouse Framework

Notes:
------
This script is designed as a one-time or repeatable bootstrap step
for initializing a data engineering platform.
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
        .appName("InfraSetup")
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

    databases = [
        (
            "monitoring_db",
            "monitoring loading during ETL various processes",
            "/mnt/c/Users/amr/Desktop/lakehouse project/monitoring/"
        ),
        (
            "bronze_db",
            "raw data with technical columns for traceability",
            "/mnt/c/Users/amr/Desktop/lakehouse project/bronze/"
        ),
        (
            "silver_db",
            "cleaned data with data quality validation",
            "/mnt/c/Users/amr/Desktop/lakehouse project/silver/"
        ),
        (
            "gold_db",
            "star schema for analytical purposes",
            "/mnt/c/Users/amr/Desktop/lakehouse project/gold/"
        )
    ]

    for db_name, comment, location in databases:

        logger.info(f"Creating database: {db_name}")

        spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS {db_name}
            COMMENT '{comment}'
            LOCATION '{location}'
        """)

        logger.info(f"Database created successfully: {db_name}")

    logger.info("All databases created successfully")

    spark.sql("SHOW DATABASES").show()

except Exception as e:

    logger.exception("Infrastructure setup failed")

    raise

finally:

    try:

        spark.stop()

        logger.info("Spark session stopped")

    except:

        pass

    execution_time = time.time() - start_time

    logger.info(f"Total execution time: {execution_time:.2f} seconds")
