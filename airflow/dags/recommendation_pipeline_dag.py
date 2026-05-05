"""
DAG — Pipeline de Recommandation de Produits en Temps Réel
Personne 3 : Ingénieur Pipeline / Airflow

Ordre :
  verifier_prerequis >> ecrire_scripts >> start_kafka_producer >> generate_recommendations >> monitoring_pipeline

Interface : http://localhost:8081  (admin / admin)

ARCHITECTURE RESEAU :
  - localhost:9092  → utilisé par les scripts sur le PC Windows
  - kafka:9093      → utilisé par les conteneurs Docker (Airflow → Kafka)
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os
import glob
import json

# ══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════
PROJECT_PATH  = "/opt/airflow/project"
JAVA_HOME     = "/usr/lib/jvm/temurin-11-jdk-amd64"
# kafka:9093 = listener Docker (annoncé par Kafka pour les conteneurs)
KAFKA_BROKER  = "kafka:9093"

default_args = {
    "owner": "personne3",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="recommendation_pipeline",
    default_args=default_args,
    description="Pipeline : Kafka -> Spark ALS -> Recommandations",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["bigdata", "kafka", "spark", "recommandation"],
)

# ══════════════════════════════════════════════════════════════
#  TACHE 0 — Verification des prerequis
# ══════════════════════════════════════════════════════════════

def verifier_prerequis():
    print("=" * 60)
    print("VERIFICATION DES PREREQUIS")
    print("=" * 60)

    fichiers_requis = {
        "Dataset CSV"      : f"{PROJECT_PATH}/data/Reviews.csv",
        "Modele ALS"       : f"{PROJECT_PATH}/als_model/metadata/part-00000",
        "Script Kafka"     : f"{PROJECT_PATH}/kafka/producer.py",
        "Script Streaming" : f"{PROJECT_PATH}/etape3_spark_streaming.py",
        "Donnees parquet"  : f"{PROJECT_PATH}/data/cleaned_reviews.parquet",
    }

    tout_ok = True
    for nom, chemin in fichiers_requis.items():
        if os.path.exists(chemin):
            print(f"  OK : {nom}")
        else:
            print(f"  MANQUANT : {nom}")
            tout_ok = False

    if not tout_ok:
        raise FileNotFoundError("Fichiers requis manquants.")

    rmse_path = f"{PROJECT_PATH}/resultats_rmse.txt"
    if os.path.exists(rmse_path):
        print("\nRESULTATS DU MODELE :")
        with open(rmse_path) as f:
            print(f.read())

    print(f"Kafka broker Docker : {KAFKA_BROKER}")
    print("Tous les prerequis sont OK.")


task_verifier = PythonOperator(
    task_id="verifier_prerequis",
    python_callable=verifier_prerequis,
    dag=dag,
)

# ══════════════════════════════════════════════════════════════
#  Scripts Python écrits sur disque (pas de f-string imbriqués)
# ══════════════════════════════════════════════════════════════

PRODUCER_SCRIPT = """
import json, time, pandas as pd, os
from kafka import KafkaProducer

KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:9093')
TOPIC_NAME   = 'reviews_stream'
CSV_PATH     = '/opt/airflow/project/data/Reviews.csv'
DELAY        = 0.05

print("Connexion a Kafka : " + KAFKA_BROKER)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
    request_timeout_ms=30000,
)
print("Connecte a Kafka !")

df = pd.read_csv(CSV_PATH, usecols=['UserId','ProductId','Score','Time']).dropna()
print("Dataset charge : " + str(len(df)) + " avis")
print("Demarrage du stream vers " + TOPIC_NAME)

for index, row in df.iterrows():
    message = {
        'UserId':    str(row['UserId']),
        'ProductId': str(row['ProductId']),
        'Score':     float(row['Score']),
        'Time':      int(row['Time'])
    }
    producer.send(TOPIC_NAME, value=message)
    if index % 500 == 0:
        print("  Message #" + str(index) + " envoye")
    time.sleep(DELAY)

producer.flush()
print("Tous les messages envoyes.")
"""

SPARK_SCRIPT = """
import os
os.environ['PYSPARK_PYTHON'] = 'python3'
os.environ['JAVA_TOOL_OPTIONS'] = ''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, hash, abs as spark_abs
from pyspark.sql.types import LongType, StructType, StructField, StringType, FloatType
from pyspark.ml.recommendation import ALSModel

KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'kafka:9093')
PROJECT_PATH = '/opt/airflow/project'

print("Kafka broker : " + KAFKA_BROKER)
print("Demarrage Spark...")

spark = SparkSession.builder \
    .appName("ALS-Streaming-Airflow") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .config("spark.driver.extraJavaOptions", "-Djavax.security.auth.useSubjectCredsOnly=false") \
    .config("spark.executor.extraJavaOptions", "-Djavax.security.auth.useSubjectCredsOnly=false") \
    .config("spark.hadoop.fs.checksum.type", "NONE") \
    .config("spark.hadoop.dfs.checksum.type", "NONE") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark demarre !")

print("Chargement du modele ALS...")
model = ALSModel.load(PROJECT_PATH + "/als_model/")
print("Modele ALS charge !")

schema = StructType([
    StructField("UserId",    StringType(), True),
    StructField("ProductId", StringType(), True),
    StructField("Score",     FloatType(),  True),
    StructField("Time",      LongType(),   True),
])

print("Connexion a Kafka : " + KAFKA_BROKER)
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "reviews_stream") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

def process_batch(batch_df, batch_id):
    count = batch_df.count()
    if count == 0:
        print("Batch " + str(batch_id) + " : vide")
        return

    print("Batch " + str(batch_id) + " : " + str(count) + " messages recus")

    batch_df = batch_df.withColumn(
        "user_idx",
        (spark_abs(hash(col("UserId"))) % 100000).cast("integer")
    )
    unique_users = batch_df.select("user_idx").distinct()
    nb = unique_users.count()
    print("  " + str(nb) + " utilisateurs -> generation recommandations...")

    recs = model.recommendForUserSubset(unique_users, 5)
    recs.show(5, truncate=False)
    recs.write.mode("append").json(PROJECT_PATH + "/output/recommendations/")
    print("  Recommandations sauvegardees !")

query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", PROJECT_PATH + "/checkpoints/streaming/") \
    .trigger(processingTime="10 seconds") \
    .start()

print("Streaming demarre — attente des messages Kafka...")
query.awaitTermination(120)
print("Streaming termine.")
"""


def ecrire_scripts():
    with open("/tmp/run_producer.py", "w") as f:
        f.write(PRODUCER_SCRIPT)
    with open("/tmp/etape3_fixed.py", "w") as f:
        f.write(SPARK_SCRIPT)
    print("Scripts ecrits dans /tmp/")
    print("Kafka broker utilise : " + KAFKA_BROKER)


task_ecrire_scripts = PythonOperator(
    task_id="ecrire_scripts",
    python_callable=ecrire_scripts,
    dag=dag,
)

# ══════════════════════════════════════════════════════════════
#  TACHE 1 — Lancer le producteur Kafka
# ══════════════════════════════════════════════════════════════

task_kafka_producer = BashOperator(
    task_id="start_kafka_producer",
    bash_command=f"""
        echo "TACHE 1 : Demarrage Kafka Producer"
        echo "Broker : {KAFKA_BROKER}"

        cd {PROJECT_PATH}
        pip install kafka-python pandas --quiet 2>/dev/null || true

        if [ ! -f "data/Reviews.csv" ]; then
            echo "ERREUR : data/Reviews.csv introuvable"
            exit 1
        fi
        echo "Dataset OK."

        echo "Lancement du producer en arriere-plan..."
        KAFKA_BROKER={KAFKA_BROKER} nohup python3 /tmp/run_producer.py \
            > /tmp/kafka_producer.log 2>&1 &
        PRODUCER_PID=$!
        echo $PRODUCER_PID > /tmp/kafka_producer.pid
        echo "Producer lance (PID: $PRODUCER_PID)"

        sleep 15

        if kill -0 $PRODUCER_PID 2>/dev/null; then
            echo "Producer actif !"
            cat /tmp/kafka_producer.log
        else
            echo "ERREUR : Le producer s est arrete"
            cat /tmp/kafka_producer.log
            exit 1
        fi
    """,
    dag=dag,
)

# ══════════════════════════════════════════════════════════════
#  TACHE 2 — Spark Streaming + recommandations ALS
# ══════════════════════════════════════════════════════════════

task_spark_streaming = BashOperator(
    task_id="generate_recommendations",
    bash_command=f"""
        echo "TACHE 2 : Spark Streaming + ALS"
        echo "Broker : {KAFKA_BROKER}"

        cd {PROJECT_PATH}

        export JAVA_HOME="{JAVA_HOME}"
        export PATH="$JAVA_HOME/bin:$PATH"
        export JAVA_TOOL_OPTIONS=""
        export PYSPARK_PYTHON="python3"

        java -version 2>&1

        mkdir -p output/recommendations
        mkdir -p checkpoints/streaming

        echo "Lancement du streaming (120 secondes)..."
        KAFKA_BROKER={KAFKA_BROKER} timeout 150 python3 /tmp/etape3_fixed.py \
            > /tmp/spark_streaming.log 2>&1 || true

        echo "Streaming termine."
        NB=$(find output/recommendations -name "*.json" 2>/dev/null | wc -l)
        echo "Fichiers JSON generes : $NB"

        if [ "$NB" -gt "0" ]; then
            echo "SUCCES : Recommandations generees !"
            find output/recommendations -name "*.json" | head -2 | while read f; do
                echo "Fichier : $f"
                head -3 "$f"
            done
        else
            echo "Aucun fichier JSON trouve."
            tail -15 /tmp/spark_streaming.log
        fi
    """,
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

# ══════════════════════════════════════════════════════════════
#  TACHE 3 — Monitoring et rapport final
# ══════════════════════════════════════════════════════════════

def monitoring_pipeline(**context):
    print("=" * 60)
    print("TACHE 3 : MONITORING DU PIPELINE")
    print("=" * 60)

    output_dir = f"{PROJECT_PATH}/output/recommendations"

    rapport = {
        "date_execution"     : datetime.now().isoformat(),
        "pipeline"           : "Kafka -> Spark ALS -> Recommandations",
        "kafka_broker_docker": KAFKA_BROKER,
        "modele"             : {
            "algorithme"     : "ALS (Alternating Least Squares)",
            "rmse_validation": 0.8114,
            "rmse_test"      : 0.8334,
            "config"         : {"rank": 20, "regParam": 0.2, "maxIter": 15},
            "donnees_train"  : "80% du dataset",
            "donnees_test"   : "10% donnees non vues",
        },
        "fichiers_generes"   : 0,
        "statut"             : "SUCCES",
    }

    fichiers = glob.glob(f"{output_dir}/**/*.json", recursive=True) + \
               glob.glob(f"{output_dir}/*.json")
    rapport["fichiers_generes"] = len(fichiers)

    print(f"\nFichiers de recommandations trouves : {len(fichiers)}")

    if len(fichiers) == 0:
        rapport["statut"] = "AVERTISSEMENT"
        print("Aucune recommandation trouvee.")
    else:
        print("\nEXEMPLES DE RECOMMANDATIONS :")
        compteur = 0
        for fichier in fichiers[:3]:
            try:
                with open(fichier) as f:
                    for ligne in f:
                        ligne = ligne.strip()
                        if ligne:
                            data = json.loads(ligne)
                            nb = len(data.get("recommendations", []))
                            print(f"  User {data.get('user_idx','?')} -> {nb} produits recommandes")
                            compteur += 1
                            if compteur >= 5:
                                break
            except Exception as e:
                print(f"  Erreur lecture : {e}")

    print("\nPERFORMANCE DU MODELE ALS :")
    print("  Algorithme      : ALS - filtrage collaboratif")
    print("  RMSE validation : 0.8114")
    print("  RMSE test final : 0.8334  (10% donnees non vues)")
    print("  Config          : rank=20, regParam=0.2, maxIter=15")

    os.makedirs(f"{PROJECT_PATH}/output", exist_ok=True)
    with open(f"{PROJECT_PATH}/output/rapport_monitoring.json", "w") as f:
        json.dump(rapport, f, indent=2, ensure_ascii=False)

    print(f"\nRapport sauvegarde : output/rapport_monitoring.json")
    print(f"STATUT FINAL : {rapport['statut']}")


task_monitoring = PythonOperator(
    task_id="monitoring_pipeline",
    python_callable=monitoring_pipeline,
    provide_context=True,
    dag=dag,
)

# ══════════════════════════════════════════════════════════════
#  ORDRE D'EXECUTION
# ══════════════════════════════════════════════════════════════
task_verifier >> task_ecrire_scripts >> task_kafka_producer >> task_spark_streaming >> task_monitoring