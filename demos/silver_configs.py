config={
    "cust_info":{
        "source_table":"bronze_db.cust_info",
        "target_table":"silver_db.cust_info",
        "primary_key":["cst_id"],
        
        "cleansing_rules":[
            "remove_duplicates",
            "drop_null_cst_id",
            "trim_strings",
            "date_casting"

        ],
        "standardisation_rules":[
            "standardize_cst_marital_status",
            "standardize_cst_gender"

        ],

        "enrichment_rules":[
            "derived_technical_columns",
            "full_name_column"

        ]

        
    },
    "prd_info":{

    }
}
