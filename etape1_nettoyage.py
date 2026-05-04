"""
ÉTAPE 1 — Nettoyage et prétraitement des données
Personne 2 : Data Scientist / Spark ML
"""

import os
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ['JAVA_TOOL_OPTIONS'] = '-Djavax.security.auth.useSubjectCredsOnly=false'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.ml.feature import StringIndexer

# ── 1. Démarrer Spark ────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("ALS-Preprocessing") \
    .config("spark.driver.extraJavaOptions",
            "-Djavax.security.auth.useSubjectCredsOnly=false") \
    .config("spark.executor.extraJavaOptions",
            "-Djavax.security.auth.useSubjectCredsOnly=false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── 2. Lire le CSV ───────────────────────────────────────────────────
df = spark.read.csv(
    "data/Reviews.csv",
    header=True,
    inferSchema=True
)

print(f"Lignes brutes : {df.count()}")
df.printSchema()

# ── 3. Garder uniquement les colonnes utiles ─────────────────────────
df = df.select("UserId", "ProductId", "Score")

# ── 4. Supprimer les nulls ───────────────────────────────────────────
df = df.dropna()
print(f"Après suppression nulls : {df.count()}")

# ── 5. Supprimer les doublons (même user + même produit) ─────────────
df = df.dropDuplicates(["UserId", "ProductId"])
print(f"Après dédoublonnage : {df.count()}")

# ── 6. Filtrer les utilisateurs avec moins de 5 avis ────────────────
user_counts = df.groupBy("UserId").agg(count("*").alias("nb_avis"))
active_users = user_counts.filter(col("nb_avis") >= 5).select("UserId")
df = df.join(active_users, on="UserId", how="inner")
print(f"Après filtre utilisateurs (≥5 avis) : {df.count()}")

# ── 7. Filtrer les produits avec moins de 5 avis ─────────────────────
product_counts = df.groupBy("ProductId").agg(count("*").alias("nb_avis"))
active_products = product_counts.filter(col("nb_avis") >= 5).select("ProductId")
df = df.join(active_products, on="ProductId", how="inner")
print(f"Après filtre produits (≥5 avis) : {df.count()}")

# ── 8. Encoder UserId et ProductId en entiers (obligatoire pour ALS) ─
indexer_user = StringIndexer(inputCol="UserId", outputCol="user_idx")
indexer_product = StringIndexer(inputCol="ProductId", outputCol="product_idx")

df = indexer_user.fit(df).transform(df)
df = indexer_product.fit(df).transform(df)

# Convertir en entiers
from pyspark.sql.functions import col as F_col
df = df.withColumn("user_idx", F_col("user_idx").cast("integer"))
df = df.withColumn("product_idx", F_col("product_idx").cast("integer"))
df = df.withColumn("Score", F_col("Score").cast("float"))

# ── 9. Vérification finale ───────────────────────────────────────────
print("\n=== Aperçu du dataset nettoyé ===")
df.select("user_idx", "product_idx", "Score").show(10)
print(f"Lignes finales : {df.count()}")

# ── 10. Sauvegarder pour réutilisation ──────────────────────────────
df.select("user_idx", "product_idx", "Score") \
  .write.mode("overwrite") \
  .parquet("data/cleaned_reviews.parquet")

print("✅ Données nettoyées sauvegardées dans data/cleaned_reviews.parquet")

spark.stop()