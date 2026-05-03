"""
ÉTAPE 2 — Entraînement et évaluation du modèle ALS
Personne 2 : Data Scientist / Spark ML
"""

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

# ── 1. Démarrer Spark ────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("ALS-Training") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── 2. Charger les données nettoyées ────────────────────────────────
df = spark.read.parquet("data/cleaned_reviews.parquet")
print(f"Lignes chargées : {df.count()}")

# ── 3. Découper en train / validation / test ─────────────────────────
# 80% entraînement, 10% validation (tuning), 10% test final
train, validation, test = df.randomSplit([0.8, 0.1, 0.1], seed=42)

print(f"Train      : {train.count()} lignes")
print(f"Validation : {validation.count()} lignes")
print(f"Test       : {test.count()} lignes")

# ── 4. Évaluateur RMSE ──────────────────────────────────────────────
evaluator = RegressionEvaluator(
    metricName="rmse",
    labelCol="Score",
    predictionCol="prediction"
)

# ── 5. Entraînement ALS avec les hyperparamètres par défaut ─────────
print("\n=== Entraînement ALS v1 (hyperparamètres de base) ===")

als = ALS(
    rank=10,            # nombre de facteurs latents
    maxIter=10,         # nombre d'itérations
    regParam=0.1,       # régularisation pour éviter l'overfitting
    userCol="user_idx",
    itemCol="product_idx",
    ratingCol="Score",
    coldStartStrategy="drop"   # ignore les users/produits inconnus
)

model = als.fit(train)

# ── 6. Évaluation sur la validation ─────────────────────────────────
predictions_val = model.transform(validation)
rmse_val = evaluator.evaluate(predictions_val)
print(f"RMSE validation (v1) : {rmse_val:.4f}")

# ── 7. Tuning des hyperparamètres ────────────────────────────────────
print("\n=== Tuning des hyperparamètres ===")

# On teste quelques combinaisons manuellement (simple et lisible)
configs = [
    {"rank": 5,  "regParam": 0.05, "maxIter": 10},
    {"rank": 10, "regParam": 0.1,  "maxIter": 10},
    {"rank": 15, "regParam": 0.1,  "maxIter": 15},
    {"rank": 20, "regParam": 0.2,  "maxIter": 15},
]

best_rmse = float("inf")
best_model = None
best_config = None

for cfg in configs:
    als_try = ALS(
        rank=cfg["rank"],
        maxIter=cfg["maxIter"],
        regParam=cfg["regParam"],
        userCol="user_idx",
        itemCol="product_idx",
        ratingCol="Score",
        coldStartStrategy="drop"
    )
    m = als_try.fit(train)
    preds = m.transform(validation)
    rmse = evaluator.evaluate(preds)
    print(f"  rank={cfg['rank']} regParam={cfg['regParam']} maxIter={cfg['maxIter']} → RMSE={rmse:.4f}")

    if rmse < best_rmse:
        best_rmse = rmse
        best_model = m
        best_config = cfg

print(f"\n✅ Meilleure config : {best_config}  → RMSE validation = {best_rmse:.4f}")

# ── 8. Évaluation finale sur le TEST (une seule fois !) ──────────────
predictions_test = best_model.transform(test)
rmse_test = evaluator.evaluate(predictions_test)
print(f"🏆 RMSE TEST FINAL : {rmse_test:.4f}")

# ── 9. Sauvegarder le modèle ─────────────────────────────────────────
best_model.write().overwrite().save("als_model/")
print("\n✅ Modèle sauvegardé dans als_model/")

# ── 10. Exemple de recommandations ──────────────────────────────────
print("\n=== Exemple Top-5 pour 3 utilisateurs ===")
user_recs = best_model.recommendForAllUsers(5)
user_recs.show(3, truncate=False)

# Sauvegarder les résultats RMSE dans un fichier texte
with open("resultats_rmse.txt", "w") as f:
    f.write(f"Meilleure config : {best_config}\n")
    f.write(f"RMSE validation  : {best_rmse:.4f}\n")
    f.write(f"RMSE test final  : {rmse_test:.4f}\n")

print("✅ Résultats RMSE sauvegardés dans resultats_rmse.txt")

spark.stop()