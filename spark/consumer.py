from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType
import redis

spark = SparkSession.builder \
    .appName("EngagementPipeline") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.6.0") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

esquema_video = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("platform", StringType(), True),
    StructField("view_count", LongType(), True),
    StructField("like_count", LongType(), True),
    StructField("dislike_count", LongType(), True),
    StructField("comment_count", LongType(), True),
    StructField("country", StringType(), True),
    StructField("locality", StringType(), True),
    StructField("upload_date", StringType(), True),
    StructField("duration", LongType(), True),
    StructField("extraction_date", StringType(), True)
])

df_kafka = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "trending_raw") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

df_json = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), esquema_video).alias("data")) \
    .select("data.*")

df_con_tiempo = df_json.withColumn("timestamp", current_timestamp())

df_limpio = df_con_tiempo.fillna({
    'view_count': 0, 
    'like_count': 0, 
    'dislike_count': 0,
    'comment_count': 0
})

df_calculo = df_limpio.withColumn("retention", lit(50))

df_engagement = df_calculo.withColumn(
    "engagement_score",
    (col("view_count") * 1) + 
    (col("retention") * 2) + 
    (col("like_count") * 5) + 
    (col("comment_count") * 10)
)

df_agregado = df_engagement \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("id"),
        col("title")
    ) \
    .max("engagement_score") \
    .withColumnRenamed("max(engagement_score)", "score_actual")

def write_redis(df, epoch_id):
    pdf = df.toPandas()
    if not pdf.empty:
        try:
            r = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
            for index, row in pdf.iterrows():
                r.zadd("ranking_viralidad", {row['title']: row['score_actual']})
            print(f"Batch {epoch_id} procesado: {len(pdf)} videos actualizados en Redis.")
        except Exception as e:
            print(f"Error guardando en Redis: {e}")

query_redis = df_agregado.writeStream \
    .foreachBatch(write_redis) \
    .outputMode("update") \
    .start()

def write_postgres(df, epoch_id):
    df.write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql://postgres:5432/viral_history") \
        .option("dbtable", "historical_events") \
        .option("user", "admin") \
        .option("password", "adminpassword") \
        .mode("append") \
        .save()

query_pg = df_engagement.writeStream \
    .foreachBatch(write_postgres) \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()