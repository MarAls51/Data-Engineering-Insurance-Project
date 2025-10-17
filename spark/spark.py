from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, pandas_udf, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import joblib, os
from dotenv import load_dotenv

# --------------------------
# Load environment variables
# --------------------------
load_dotenv()
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "insurance_raw")

# --------------------------
# Spark session
# --------------------------
spark = SparkSession.builder.appName("InsuranceRiskStreaming").getOrCreate()

# --------------------------
# Schema for incoming JSON
# --------------------------
schema = StructType([
    StructField("age", IntegerType(), True),
    StructField("sex", StringType(), True),
    StructField("bmi", DoubleType(), True),
    StructField("children", IntegerType(), True),
    StructField("smoker", StringType(), True),
    StructField("region", StringType(), True),
    StructField("event_time", DoubleType(), True),
    StructField("charges", DoubleType(), True)
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
def preprocess_spark(df):
    df = df.dropDuplicates()
    for c in df.columns:
        df = df.filter(col(c).isNotNull())
    
    df = df.withColumnRenamed("sex", "sex_original")
    df = df.withColumnRenamed("smoker", "smoker_original")
    
    df = df.withColumn("sex", when(col("sex_original") == "male", 0).otherwise(1))
    df = df.withColumn("smoker", when(col("smoker_original") == "no", 0).otherwise(1))
    
    df = df.withColumnRenamed("region", "region_original")

    regions = ["northeast","northwest","southeast","southwest"]
    for r in regions[1:]:
        df = df.withColumn(f"region_{r}", (col("region_original") == r).cast("int"))
    
    return df
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
    preds = model.predict(X)
    return pd.Series(preds)

predict_udf = pandas_udf(predict_risk, StringType())

# --------------------------
# Apply prediction
# --------------------------
df_with_pred = df_preprocessed.withColumn(
    "risk",
    predict_udf(
        "age","sex","bmi","children","smoker",
        "region_northwest","region_southeast","region_southwest"
    )
)

# --------------------------
# Write predictions to DB
# --------------------------
jdbc_url = f"jdbc:postgresql://{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}"

def write_to_data_schema(batch_df, batch_id):
    batch_df.select(
        "age",
        "sex_original",
        "bmi",
        "children",
        "smoker_original",
        "region_original",
        "charges",
        "event_time",
        "risk"
    ).write.format("jdbc") \
     .option("url", jdbc_url) \
     .option("dbtable", "data.insurance_predictions") \
     .option("user", REDSHIFT_USER) \
     .option("password", REDSHIFT_PASSWORD) \
     .mode("append").save()
    
def write_to_ml_schema(batch_df, batch_id):
    batch_df.select(
        "age",
        "sex",
        "bmi",
        "children",
        "smoker",
        "region_northwest",
        "region_southeast",
        "region_southwest",
        "charges","risk"
    ).write.format("jdbc") \
     .option("url", jdbc_url) \
     .option("dbtable", "ml.insurance_features") \
     .option("user", REDSHIFT_USER) \
     .option("password", REDSHIFT_PASSWORD) \
     .mode("append").save()

stream_data = df_with_pred.writeStream.foreachBatch(write_to_data_schema).start()
stream_ml = df_with_pred.writeStream.foreachBatch(write_to_ml_schema).start()

stream_data.awaitTermination()
stream_ml.awaitTermination()
