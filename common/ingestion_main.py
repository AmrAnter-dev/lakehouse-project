"""
Bronze Layer Ingestion Pipeline (Lakehouse Architecture)
========================================================

This script is responsible for ingesting raw data from source systems
(CRM and ERP) into the Bronze layer of a Lakehouse architecture using
Apache Spark and Delta Lake.

Overview:
---------
The Bronze layer acts as the raw ingestion layer in a Medallion Architecture.
Data is loaded with minimal transformation while adding technical metadata
for traceability and auditing.

Key Responsibilities:
---------------------
- Read raw CSV files from CRM and ERP source systems
- Add technical metadata columns:
    - batch_id (UUID for traceability)
    - source_system (CRM / ERP)
    - source_file (origin tracking)
    - ingestion_ts (load timestamp)
- Write data into Delta tables in the Bronze database
- Maintain audit logs for success and failure cases
- Ensure fault-tolerant ingestion (continue pipeline on failures)

Technologies Used:
------------------
- Apache Spark (PySpark)
- Delta Lake
- Python logging (custom PipelineLogger)
- Custom Audit Logging Framework
- UUID-based batch tracking

Data Architecture Layer:
------------------------
Bronze Layer → Raw ingestion (append-only / overwrite in this setup)
Silver Layer → Cleaned and validated data
Gold Layer   → Analytical / star schema

Error Handling:
--------------
- Each file ingestion is isolated (failure does not stop pipeline)
- Errors are logged using logger.exception()
- Failures are recorded in audit table

Execution Flow:
---------------
1. Start Spark session with Delta support
2. Initialize audit logger
3. Loop through CRM and ERP file configurations
4. Ingest each dataset into Bronze Delta tables
5. Log success or failure per dataset
6. Stop Spark session and log execution metrics

Output:
------
- Delta tables in: bronze_db.<table_name>
- Audit logs in: monitoring_db.audit_table
- Pipeline logs in logs/pipeline.log

Author:
------
Data Engineering Pipeline Framework

Note:
-----
Designed for local Spark (WSL) development and scalable to
Databricks or cloud-based Lakehouse environments.

"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import uuid
from datetime import datetime
from delta import configure_spark_with_delta_pip
from bronze_configs import CRM_FILE_PATHS, ERP_FILE_PATHS
from audit_logger import AuditLogger
from pipeline_logger import PipelineLogger
import time


logger = PipelineLogger.get_logger(__name__)

start_time = time.time()

try:
    logger.info("Starting Spark session")
    # =========================
    # Spark Session (Delta enabled)
    # =========================
    builder = (
        SparkSession.builder
        .appName("load_bronze")
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
        .config("spark.sql.parquet.compression.codec", "uncompressed")
        .enableHiveSupport()
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()


    logger.info("Spark session created successfully")

    auditlogger = AuditLogger(spark)
    # =========================
    # Bronze Ingestion Function
    # =========================
    def ingest_to_bronze(source_system, file_configs):

        for name, path in file_configs.items():

            batch_id = str(uuid.uuid4())
            load_start_time = datetime.now()

            try:
                # -------------------------
                # Read source file
                # -------------------------
                df = spark.read.format('csv') \
                    .option('header', 'true') \
                    .load(path)
                    
                logger.info(f'reading file:{name}.csv ')
                # -------------------------
                # Add technical columns
                # -------------------------
                df = df.withColumn('batch_id', lit(batch_id)) \
                    .withColumn('source_system', lit(source_system)) \
                    .withColumn('source_file', input_file_name()) \
                    .withColumn('ingestion_ts', current_timestamp())

                # -------------------------
                # Write to Bronze Delta table
                # -------------------------
                df.write.format('delta') \
                    .mode('overwrite') \
                    .option('overwriteSchema', 'true') \
                    .saveAsTable(f"bronze_db.{name}")
                    
                logger.info(f'loading table: {name} into bronze layer')
                
                load_end_time = datetime.now()

                # -------------------------
                # Audit success
                # -------------------------
                spark.sql("USE monitoring_db")
                auditlogger.success(
                batch_id=batch_id,
                table_name=f"bronze_db.{name}",
                start_time=load_start_time,
                end_time=load_end_time,
                row_count=df.count()
                )
                
                logger.info(f"Successfully loaded: {source_system}.{name}")
                

            except Exception as e:
                
                logger.exception(f"Failed loading {source_system}.{name}" )
                
                load_end_time = datetime.now()

                # -------------------------
                # Audit failure
                # -------------------------
                spark.sql("USE monitoring_db")
                auditlogger.failed(
                batch_id=batch_id,
                table_name=f"bronze_db.{name}",
                start_time=load_start_time,
                end_time=load_end_time,
                error_message=str(e)
                )

                logger.info(f"Failed loading {source_system}.{name}: {e}")
                # continue pipeline instead of stopping everything
                continue


    # =========================
    # Run Ingestion Jobs
    # =========================
    ingest_to_bronze("CRM", CRM_FILE_PATHS)
    ingest_to_bronze("ERP", ERP_FILE_PATHS)
    
except Exception as e:

    logger.exception("loading bronze layer failed")

    raise

finally:

    try:

        spark.stop()

        logger.info("Spark session stopped")

    except:

        pass

    execution_time = time.time() - start_time
    logger.info('bronze layer loaded successfully')
    logger.info(f"Total execution time: {execution_time:.2f} seconds")
     
