class PipelineEngine:

    def __init__(self, spark, config,registries):
                 
                
                 

        self.spark = spark
        self.config = config

        self.registries = registries
    def _load_data(self, source_table):
        return self.spark.table(source_table)
    

    def _apply_stage(self, df, stage_name, rules):

        try:
            registry = self.registries[stage_name]
        except KeyError:
            raise ValueError(f"Stage not registered: {stage_name}")
        
        for rule in rules:
            rule_name = rule['name']
            params = rule.get('params', {})
            
            if rule_name not in registry:
                raise ValueError(f"Rule not found: {rule_name} in stage {stage_name}")

            try:
                df = registry[rule_name](df, **params)

            except Exception as e:
                raise RuntimeError(
                    f"Error executing rule {rule_name} in stage {stage_name}: {str(e)}"
    )

        return df
    
    def _write_data(self, df, target_table):
        df.write.mode("overwrite").saveAsTable(target_table)

    def run(self, table_name):

        if table_name not in self.config:
            raise ValueError(f"Table not found in config: {table_name}")
        
        table_config = self.config[table_name]

        df = self._load_data(table_config["source_table"])

     

        for stage_name, rules in table_config['stages'].items():

            df = self._apply_stage(
                df,
                stage_name,
                rules
            )

        self._write_data(df, table_config["target_table"])

    def run_all(self):
        for table_name in self.config:
            self.run(table_name)


            
