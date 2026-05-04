"""
ÉTAPE 3 — Spark Structured Streaming depuis Kafka
Personne 2 : Data Scientist / Spark ML
⚠️  À lancer APRÈS que la Personne 1 (Kafka) ait tout mis en place
"""

import os
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ['JAVA_TOOL_OPTIONS'] = '-Djavax.security.auth.useSubjectCredsOnly=false'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import LongType, StructType, StructField, StringType, FloatType
from pyspark.ml.recommendation import ALSModel

# ── 1. Démarrer Spark avec le connecteur Kafka ────────────────────────
spark = SparkSession.builder \
    .appName("ALS-Streaming") \
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0"
    ) \
    .config("spark.driver.extraJavaOptions",
            "-Djavax.security.auth.useSubjectCredsOnly=false") \
    .config("spark.executor.extraJavaOptions",
            "-Djavax.security.auth.useSubjectCredsOnly=false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── 2. Charger le modèle ALS déjà entraîné ───────────────────────────
model = ALSModel.load("als_model/")
print("✅ Modèle ALS chargé depuis als_model/")

# ── 3. Charger le mapping UserId → user_idx ──────────────────────────
# (créé par l'étape 1, contient la correspondance string → entier)
# Le parquet ne contient que user_idx — on lit directement les user_idx uniques
mapping = spark.read.parquet("data/cleaned_reviews.parquet") \
    .select("user_idx") \
    .distinct()
print("✅ Mapping user_idx chargé")


# ── 4. Définir le schéma des messages Kafka ──────────────────────────
schema = StructType([
    StructField("UserId",    StringType(), True),
    StructField("ProductId", StringType(), True),
    StructField("Score",     FloatType(),  True),
    StructField("Time",      LongType(),   True),
])

# ── 5. Lire le flux Kafka ────────────────────────────────────────────
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reviews_stream") \
    .option("startingOffsets", "latest") \
    .load()

# ── 6. Parser le JSON reçu ───────────────────────────────────────────
parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# ── 7. Fonction appelée pour chaque micro-batch ──────────────────────
def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    print(f"\n--- Batch {batch_id} : {batch_df.count()} nouveaux avis ---")

    # Joindre avec le mapping pour obtenir user_idx depuis UserId
   # Convertir UserId string en entier directement
    from pyspark.sql.functions import hash, abs as spark_abs
    batch_df = batch_df.withColumn(
        "user_idx", 
        (spark_abs(hash(col("UserId"))) % 100000).cast("integer")
    )
    unique_users = batch_df.select("user_idx").distinct()

    nb_users = unique_users.count()
    if nb_users == 0:
        print("  ⚠️  Aucun user connu dans ce batch")
        return

    print(f"  👤 {nb_users} utilisateurs connus → génération des recommandations...")

    # Générer les Top-5 recommandations pour ces users
    recs = model.recommendForUserSubset(unique_users, 5)

    # Afficher les résultats
    recs.show(truncate=False)

    # Sauvegarder en JSON pour la Personne 4
    recs.write \
        .mode("append") \
        .json("output/recommendations/")

# ── 8. Lancer le streaming ───────────────────────────────────────────
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "checkpoints/streaming/") \
    .trigger(processingTime="10 seconds") \
    .start()

print("🚀 Streaming démarré — en attente de messages Kafka...")
query.awaitTermination()