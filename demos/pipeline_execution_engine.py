class PipelineEngine:

    def __init__(self, spark, config,
                 cleansing_registry,
                 standardization_registry,
                 enrichment_registry):

        self.spark = spark
        self.config = config

        self.registries = {
            "cleansing_rules": cleansing_registry,
            "standardisation_rules": standardization_registry,
            "enrichment_rules": enrichment_registry
        }
    def load_data(self, source_table):
        return self.spark.table(source_table)
    

    def apply_stage(self, df, stage_name, rules):

        registry = self.registries[stage_name]

        for rule in rules:
            if rule not in registry:
                raise Exception(f"Rule not found: {rule}")

            df = registry[rule](df)

        return df
    
    def write_data(self, df, target_table):
        df.write.mode("overwrite").saveAsTable(target_table)

    def run(self, table_name):

        table_config = self.config[table_name]

        df = self.load_data(table_config["source_table"])

        df = self.apply_stage(
            df,
            "cleansing_rules",
            table_config.get("cleansing_rules", [])
        )

        df = self.apply_stage(
            df,
            "standardisation_rules",
            table_config.get("standardisation_rules", [])
        )

        df = self.apply_stage(
            df,
            "enrichment_rules",
            table_config.get("enrichment_rules", [])
        )

        self.write_data(df, table_config["target_table"])

    def run_all(self):
        for table_name in self.config.keys():
            self.run(table_name)


            
engine = PipelineEngine(
    spark,
    config,
    CLEANSING_FUNCTIONS,
    STANDARDIZATION_FUNCTIONS,
    ENRICHMENT_FUNCTIONS
)

engine.run("cust_info")
