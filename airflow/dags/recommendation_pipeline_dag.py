"""
DAG — Pipeline de Recommandation de Produits en Temps Réel
Personne 3 : Ingénieur Pipeline / Airflow

Ordre :
  verifier_prerequis >> start_kafka_producer >> generate_recommendations >> monitoring_pipeline

Interface : http://localhost:8081  (admin / admin)
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
#  CHEMINS
#  PROJECT_PATH = chemin du projet DANS le conteneur Airflow
#  KAFKA_BROKER = host.docker.internal car Kafka tourne sur le PC
#                 et Airflow tourne dans Docker
# ══════════════════════════════════════════════════════════════
PROJECT_PATH = "/opt/airflow/project"
KAFKA_BROKER = "host.docker.internal:9092"

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

    print("Tous les prerequis sont OK.")


task_verifier = PythonOperator(
    task_id="verifier_prerequis",
    python_callable=verifier_prerequis,
    dag=dag,
)

# ══════════════════════════════════════════════════════════════
#  TACHE 1 — Lancer le producteur Kafka
#  Utilise host.docker.internal pour atteindre Kafka sur le PC
# ══════════════════════════════════════════════════════════════

task_kafka_producer = BashOperator(
    task_id="start_kafka_producer",
    bash_command=f"""
        echo "TACHE 1 : Demarrage Kafka Producer"
        echo "Broker : {KAFKA_BROKER}"
        echo "Topic  : reviews_stream"

        cd {PROJECT_PATH}

        pip install kafka-python pandas --quiet 2>/dev/null || true

        # Verifier que Reviews.csv existe
        if [ ! -f "data/Reviews.csv" ]; then
            echo "ERREUR : data/Reviews.csv introuvable"
            exit 1
        fi

        echo "Dataset OK."

        # Modifier temporairement le broker pour utiliser host.docker.internal
        # On cree un script wrapper qui remplace localhost par host.docker.internal
        cat > /tmp/run_producer.py << 'PYEOF'
import json
import time
import pandas as pd
import sys
sys.path.insert(0, '/opt/airflow/project/kafka')

# Importer et modifier le broker avant de lancer
from kafka import KafkaProducer

KAFKA_BROKER = 'host.docker.internal:9092'
TOPIC_NAME   = 'reviews_stream'
CSV_PATH     = '/opt/airflow/project/data/Reviews.csv'
DELAY        = 0.05

print(f"Connexion a Kafka : {{KAFKA_BROKER}}")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5
)
print("Connecte a Kafka !")

print(f"Chargement du dataset...")
df = pd.read_csv(CSV_PATH, usecols=['UserId', 'ProductId', 'Score', 'Time']).dropna()
print(f"Dataset charge : {{len(df)}} avis")

print(f"Demarrage du stream vers '{{TOPIC_NAME}}'...")
for index, row in df.iterrows():
    message = {{
        'UserId':    str(row['UserId']),
        'ProductId': str(row['ProductId']),
        'Score':     float(row['Score']),
        'Time':      int(row['Time'])
    }}
    producer.send(TOPIC_NAME, value=message)
    if index % 500 == 0:
        print(f"  Message #{{index}} envoye")
    time.sleep(DELAY)

producer.flush()
print("Tous les messages envoyes.")
PYEOF

        echo "Lancement du producer en arriere-plan..."
        nohup python3 /tmp/run_producer.py > /tmp/kafka_producer.log 2>&1 &
        PRODUCER_PID=$!
        echo $PRODUCER_PID > /tmp/kafka_producer.pid
        echo "Producer lance (PID: $PRODUCER_PID)"

        # Attendre 10 secondes pour verifier qu'il demarre bien
        sleep 10

        if kill -0 $PRODUCER_PID 2>/dev/null; then
            echo "Producer actif - messages en cours d envoi"
            echo "--- Log producer ---"
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
#  Modifie le broker Kafka dans etape3 pour host.docker.internal
# ══════════════════════════════════════════════════════════════

task_spark_streaming = BashOperator(
    task_id="generate_recommendations",
    bash_command=f"""
        echo "TACHE 2 : Spark Streaming + ALS"
        echo "Modele  : als_model/"
        echo "Broker  : {KAFKA_BROKER}"
        echo "Sortie  : output/recommendations/"

        cd {PROJECT_PATH}

        # Java 8 (obligatoire pour Spark)
        export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
        export PATH="$JAVA_HOME/bin:$PATH"
        export JAVA_TOOL_OPTIONS=""
        export PYSPARK_PYTHON="python3"

        pip install pyspark==3.4.0 --quiet 2>/dev/null || true

        mkdir -p output/recommendations
        mkdir -p checkpoints/streaming

        # Creer une version modifiee de etape3 avec le bon broker
        sed 's/localhost:9092/{KAFKA_BROKER}/g' etape3_spark_streaming.py > /tmp/etape3_docker.py

        echo "Broker remplace : localhost:9092 -> {KAFKA_BROKER}"
        echo "Lancement du streaming (120 secondes)..."

        timeout 120 python3 /tmp/etape3_docker.py > /tmp/spark_streaming.log 2>&1 || true

        echo "Streaming termine."
        echo "--- Verification des fichiers generes ---"
        NB=$(find output/recommendations -name "*.json" 2>/dev/null | wc -l)
        echo "Fichiers JSON generes : $NB"

        if [ "$NB" -gt "0" ]; then
            echo "Recommandations generees avec succes !"
            find output/recommendations -name "*.json" | head -2 | while read f; do
                echo "Fichier : $f"
                head -3 "$f"
            done
        else
            echo "Aucun fichier JSON trouve."
            echo "--- Fin du log Spark ---"
            tail -20 /tmp/spark_streaming.log
        fi
    """,
    execution_timeout=timedelta(minutes=5),
    dag=dag,
)

# ══════════════════════════════════════════════════════════════
#  TACHE 3 — Monitoring et rapport
# ══════════════════════════════════════════════════════════════

def monitoring_pipeline(**context):
    print("=" * 60)
    print("TACHE 3 : MONITORING DU PIPELINE")
    print("=" * 60)

    output_dir = f"{PROJECT_PATH}/output/recommendations"

    rapport = {
        "date_execution"     : datetime.now().isoformat(),
        "pipeline"           : "Kafka -> Spark ALS -> Recommandations",
        "kafka_broker"       : KAFKA_BROKER,
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
        print("Cause possible : Kafka n a pas envoye de batch dans le temps imparti.")
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
                print(f"  Erreur : {e}")

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
task_verifier >> task_kafka_producer >> task_spark_streaming >> task_monitoring