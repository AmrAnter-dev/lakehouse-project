"""
Bronze Layer Source Configuration (CRM & ERP File Paths)
========================================================

This module defines the source file paths for CRM and ERP systems
used in the Bronze layer ingestion pipeline of the Lakehouse architecture.

Overview:
---------
These configurations act as the single source of truth for raw data locations.
They are consumed by the Bronze ingestion pipeline to dynamically load
data into Spark and persist it into Delta tables.

Data Sources:
------------
1. CRM System (Customer Relationship Management)
   - Customer master data
   - Product information
   - Sales transactions

2. ERP System (Enterprise Resource Planning)
   - Customer reference data
   - Location mapping data
   - Product category mapping

Structure:
---------
Each dictionary maps:
    table_name → absolute file path

Example:
-------
"cust_info" → customer information CSV file path

Usage:
------
These configs are imported into the ingestion pipeline:

    from bronze_configs import CRM_FILE_PATHS, ERP_FILE_PATHS

    ingest_to_bronze("CRM", CRM_FILE_PATHS)

Design Principles:
------------------
- Centralized configuration management
- Easy extensibility for new data sources
- Decouples ingestion logic from file locations
- Supports scalable lakehouse architecture

Author:
------
Data Engineering Lakehouse Framework
"""
CRM_FILE_PATHS = {
    "cust_info":"/mnt/c/Users/amr/Desktop/DataWarehouse project/datasets/source_crm/cust_info.csv",
    "prd_info":"/mnt/c/Users/amr/Desktop/DataWarehouse project/datasets/source_crm/prd_info.csv",
    "sales_details":"/mnt/c/Users/amr/Desktop/DataWarehouse project/datasets/source_crm//sales_details.csv"
   
    
    
}
ERP_FILE_PATHS={
     "cust_az12":"/mnt/c/Users/amr/Desktop/DataWarehouse project/datasets/source_erp/CUST_AZ12.csv",
    "loc_a101":"/mnt/c/Users/amr/Desktop/DataWarehouse project/datasets/source_erp/LOC_A101.csv",
    "px_cat_g1v2":"/mnt/c/Users/amr/Desktop/DataWarehouse project/datasets/source_erp/PX_CAT_G1V2.csv"
}
