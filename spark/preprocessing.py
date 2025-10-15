import pandas as pd
from pyspark.sql.functions import col, when

def preprocess_spark(df):
    """
    Takes a Spark DataFrame and returns a preprocessed DataFrame ready for prediction.
    Handles:
        - Drop duplicates & NaNs
        - Encode sex and smoker
        - One-hot encode region
    """
    # --- Basic sanity ---
    df = df.dropDuplicates()
    for c in df.columns:
        df = df.filter(col(c).isNotNull())

    # --- Encode sex and smoker ---
    df = df.withColumn("sex", when(col("sex") == "male", 0).otherwise(1))
    df = df.withColumn("smoker", when(col("smoker") == "no", 0).otherwise(1))

    # --- One-hot encode region ---
    regions = ["northeast", "northwest", "southeast", "southwest"]
    for r in regions:
        df = df.withColumn(f"region_{r}", (col("region") == r).cast("double"))

    df = df.drop("region")
    return df
