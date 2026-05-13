CLEANSING_FUNCTIONS={
     "remove_duplicates":deduplicate_latest,
     "drop_null_cst_id":drop_null_cst_id,
     "trim_strings":trim_strings,
     "date_casting":date_casting
}
STANDARDIZATION_FUNCTIONS={
    "standardize_cst_marital_status":standardize_marital_status,
    "standardize_cst_gender":standardize_gender
}
ENRICHMENT_FUNCTIONS={
    "full_name_column":derive_column_full_name,
    "derived_technical_columns":derive_technical_columns

}

stages={
    "cleansing_rules":CLEANSING_FUNCTIONS,
    "standardisation_rules":STANDARDIZATION_FUNCTIONS,
    "enrichment_rules":ENRICHMENT_FUNCTIONS
}
