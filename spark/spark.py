from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import joblib
from dotenv import load_dotenv
import os
from preprocessing import preprocess_spark

# --------------------------
# Load environment variables
# --------------------------
load_dotenv()
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "insurance_raw")

# --------------------------
# Spark session
# --------------------------
spark = SparkSession.builder.appName("InsuranceRiskStreaming").getOrCreate()

# --------------------------
# Schema for incoming JSON
# --------------------------
schema = StructType([
    StructField("age", DoubleType(), True),
    StructField("sex", StringType(), True),
    StructField("bmi", DoubleType(), True),
    StructField("children", DoubleType(), True),
    StructField("smoker", StringType(), True),
    StructField("region", StringType(), True),
    StructField("event_time", DoubleType(), True)
])

# --------------------------
# Read stream from Kafka
# --------------------------
df_kafka = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# --------------------------
# Apply preprocessing
# --------------------------
df_preprocessed = preprocess_spark(df_parsed)

# --------------------------
# Load trained ML model
# --------------------------
model = joblib.load("./models/insurance_risk_model.pkl")

# --------------------------
# Pandas UDF for prediction 
# --------------------------
def predict_risk(*cols):
    import pandas as pd
    
    X = pd.DataFrame(list(zip(*cols)), columns=[
        "age","sex","bmi","children","smoker",
        "region_northwest","region_southeast","region_southwest" 
    ])
    
    predictions = model.predict(X)
    
    return pd.Series(predictions) 

predict_udf = pandas_udf(predict_risk, StringType())

# --------------------------
# Apply prediction
# --------------------------
df_with_pred = df_preprocessed.withColumn("risk", predict_udf(
    "age","sex","bmi","children","smoker",
    "region_northwest","region_southeast","region_southwest"
))

# --------------------------
# Write predictions to PostgreSQ
# --------------------------
def write_batch(batch_df, batch_id):
    print(f"Batch {batch_id} preview:")
    batch_df.show(truncate=False) 
    
    batch_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}") \
        .option("dbtable", "data.insurance_predictions") \
        .option("user", REDSHIFT_USER) \
        .option("password", REDSHIFT_PASSWORD) \
        .mode("append") \
        .save()

df_with_pred.writeStream \
    .foreachBatch(write_batch) \
    .start() \
    .awaitTermination()