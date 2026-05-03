"""
ÉTAPE 3 — Spark Structured Streaming depuis Kafka
Personne 2 : Data Scientist / Spark ML
⚠️  À lancer APRÈS que la Personne 1 (Kafka) ait tout mis en place
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import LongType, StructType, StructField, StringType, FloatType
from pyspark.ml.recommendation import ALSModel

# ── 1. Démarrer Spark avec le connecteur Kafka ────────────────────────
spark = SparkSession.builder \
    .appName("ALS-Streaming") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
    ) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── 2. Charger le modèle ALS déjà entraîné ───────────────────────────
model = ALSModel.load("als_model/")
print("✅ Modèle ALS chargé depuis als_model/")

# ── 3. Définir le schéma des messages Kafka ──────────────────────────
# Format attendu depuis le Producer Kafka (Personne 1) :
# JSON : {"user_idx": 123, "product_idx": 456, "score": 4.0}
schema = StructType([
    StructField("UserId",    StringType(), True),
    StructField("ProductId", StringType(), True),
    StructField("Score",     FloatType(),  True),
    StructField("Time",      LongType(),   True),
])

# ── 4. Lire le flux Kafka ────────────────────────────────────────────
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "reviews_stream") \
    .option("startingOffsets", "latest") \
    .load()

# ── 5. Parser le JSON reçu ───────────────────────────────────────────
parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("user_idx",    col("UserId").cast("integer")) \
    .withColumn("product_idx", col("ProductId").cast("integer"))

# ── 6. Fonction appelée pour chaque micro-batch ──────────────────────
def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    print(f"\n--- Batch {batch_id} : {batch_df.count()} nouveaux avis ---")

    # Extraire les users uniques dans ce batch
    unique_users = batch_df.select("user_idx").distinct()

    # Générer les Top-5 recommandations pour ces users
    recs = model.recommendForUserSubset(unique_users, 5)

    # Afficher les résultats
    recs.show(truncate=False)

    # Sauvegarder en JSON (pour que la Personne 4 puisse les lire)
    recs.write \
        .mode("append") \
        .json("output/recommendations/")

# ── 7. Lancer le streaming ───────────────────────────────────────────
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "checkpoints/streaming/") \
    .trigger(processingTime="10 seconds") \
    .start()

print("🚀 Streaming démarré — en attente de messages Kafka...")
query.awaitTermination()