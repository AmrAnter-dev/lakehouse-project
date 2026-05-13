from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

#Basic transformation layer (MVP)
def deduplicate_latest(df, key, order_col):
    window_spec = Window.partitionBy(key).orderBy(F.desc(order_col))

    return (
        df.withColumn("rn", F.row_number().over(window_spec))
          .filter(F.col("rn") == 1)
          .drop("rn")
    )

def drop_null_cst_id(df):
    return df.filter(F.col("cst_id").isNotNull())




def trim_strings(df):
    for col_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(col_name, F.lower(F.trim(F.col(col_name))))
    return df



def date_casting(df, date_columns=None):
    if not date_columns:
        return df

    for col_name in date_columns:
        df = df.withColumn(col_name, F.to_date(F.col(col_name)))
    return df



def standardize_marital_status(df):
    return df.withColumn(
        "cst_marital_status",
        F.when(F.lower(F.col("cst_marital_status")).isin("m", "married"), "Married")
         .when(F.lower(F.col("cst_marital_status")).isin("s", "single"), "Single")
         .otherwise("Unknown")
    )

def standardize_gender(df):
    return df.withColumn(
        "cst_gender",
        F.when(F.lower(F.col("cst_gender")).isin("m", "male"), "Male")
         .when(F.lower(F.col("cst_gender")).isin("f", "female"), "Female")
         .otherwise("N/A")
    )


def derive_column_full_name(df):
    return df.withColumn(
        "full_name",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
    )



def derive_technical_columns(df):
    return (
        df.withColumn("is_active", F.when(F.to_date(F.col('cst_create_date')).isNotNull(),1).otherwise(0))
          .withColumn("is_current", F.when(F.col("rn") == 1, 1).otherwise(0))
          .withColumn("source_system", F.lit("bronze"))
    )

#Data Quality Functions
#Schema Enforcement
#CD / Merge Logic
#Logging-aware wrappers
#Error handling layer
