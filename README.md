# Système de recommandation de produits en temps réel
#  Partie 1 — Kafka + Docker Compose
> Ingestion des données en temps réel | Projet Big Data

---

## 🎯 Ce que fait cette partie

Ce module simule un **flux de données en temps réel** à partir du dataset Amazon Fine Food Reviews.  
Il lit les 568 454 avis du CSV et les envoie un par un dans un **topic Kafka** (`reviews_stream`),  
comme si de vrais utilisateurs notaient des produits en direct.

**Format de chaque message envoyé :**
```json
{
  "UserId":    "A3SGXH7AUHU8GW",
  "ProductId": "B001E4KFG0",
  "Score":     5.0,
  "Time":      1303862400
}
```

---

## 🗂️ Structure des fichiers

```
projet-recommandation_BIG_DATA/
├── docker-compose.yml       ← Lance Zookeeper + Kafka
├── data/
│   └── Reviews.csv          ← Dataset Amazon (à télécharger, voir ci-dessous)
└── kafka/
    ├── producer.py          ← Script d'ingestion temps réel
    └── requirements.txt     ← Dépendances Python
```

---

## ⚙️ Prérequis

Avant de commencer, assure-toi d'avoir installé :

| Outil | Version recommandée | Lien |
|-------|-------------------|------|
| Docker Desktop | Dernière version | https://www.docker.com/products/docker-desktop |
| Python | 3.10+ | https://www.python.org/downloads |
| Git | Dernière version | https://git-scm.com/download/win |

---

## 🚀 Installation et lancement (étape par étape)

### 1. Cloner le repo

```bash
git clone https://github.com/salmaaz29/projet-recommandation_BIG_DATA.git
cd projet-recommandation_BIG_DATA
```

### 2. Télécharger le dataset

Le fichier `Reviews.csv` est trop volumineux pour GitHub (~300 MB).  
Télécharge-le manuellement depuis Kaggle :

🔗 **https://www.kaggle.com/snap/amazon-fine-food-reviews**

Place le fichier `Reviews.csv` dans le dossier `data/` :
```
projet-recommandation_BIG_DATA/
└── data/
    └── Reviews.csv   ✅
```

### 3. Installer les dépendances Python

```bash
pip install kafka-python pandas
```

### 4. Démarrer Docker Desktop

Ouvre **Docker Desktop** et attends que l'icône soit verte (30-60 secondes).

### 5. Lancer Kafka et Zookeeper

```bash
docker-compose up -d
```

Vérifie que les 2 conteneurs tournent :
```bash
docker ps
```

Tu dois voir :
```
CONTAINER ID   IMAGE                              STATUS
xxxxxxxxxxxx   confluentinc/cp-kafka:7.5.0        Up X seconds
xxxxxxxxxxxx   confluentinc/cp-zookeeper:7.5.0    Up X seconds
```

### 6. Lancer le producer (flux temps réel)

```bash
cd kafka
python producer.py
```

Tu dois voir les messages s'envoyer :
```
✅ Connecté à Kafka sur localhost:9092
📂 Chargement du dataset depuis ../data/Reviews.csv...
✅ Dataset chargé : 568454 avis disponibles
🚀 Démarrage du stream vers le topic 'reviews_stream'...

   ✉️  Message #0 envoyé → {'UserId': 'A3SGXH7AUHU8GW', ...}
   ✉️  Message #100 envoyé → ...
```

---

## ✅ Vérifier que les messages arrivent dans Kafka

Ouvre un **deuxième terminal** et lance :

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic reviews_stream \
  --from-beginning
```

Tu dois voir les messages JSON défiler en temps réel. Si c'est le cas → tout fonctionne.

Arrête avec `Ctrl+C`.

---

# Partie B — Spark MLlib + ALS (Data Scientist)
## Système de Recommandation Big Data 

---

## 🎯 Ce que fait cette partie

Ce module constitue le **cerveau du système de recommandation**.  
Il nettoie les données, entraîne un modèle ALS (filtrage collaboratif) et génère des recommandations Top-5 en temps réel depuis Kafka.

---

## ✅ Résultats obtenus

| Métrique | Valeur |
|---|---|
| Lignes brutes | 568 454 |
| Lignes après nettoyage | 187 695 |
| Algorithme | ALS (Alternating Least Squares) |
| Meilleure config | rank=20, regParam=0.2, maxIter=15 |
| RMSE validation | 0.8114 |
| **RMSE test final** | **0.8334** |

---

## 🗂️ Structure des fichiers

```
projet-recommandation_BIG_DATA/
├── etape1_nettoyage.py          ← Nettoyage et prétraitement des données
├── etape2_als_training.py       ← Entraînement et évaluation du modèle ALS
├── etape3_spark_streaming.py    ← Streaming Kafka → recommandations temps réel
├── als_model/                   ← Modèle ALS entraîné (itemFactors, userFactors, metadata)
├── data/
│   ├── Reviews.csv              ← Dataset Amazon (à télécharger depuis Kaggle)
│   └── cleaned_reviews.parquet  ← Données nettoyées (généré par étape 1)
├── output/
│   └── recommendations/         ← Recommandations JSON (généré par étape 3)
├── checkpoints/                 ← Checkpoints Spark Streaming
├── resultats_rmse.txt           ← Résultats officiels RMSE
├── docker-compose-spark.yml     ← Configuration Docker Spark
└── .gitignore                   ← Fichiers exclus de Git
```

---

## ⚙️ Prérequis

| Outil | Version |
|---|---|
| Python | 3.12.7 |
| Java | **8** (jdk-8.0.482.8-hotspot) — OBLIGATOIRE |
| PySpark | 3.4.0 |
| Hadoop | 3.3.6 (winutils.exe) |
| Docker Desktop | Dernière version |

---

## 🚀 Installation et lancement

### 1. Cloner le repo
```bash
git clone https://github.com/salmaaz29/projet-recommandation_BIG_DATA.git
cd projet-recommandation_BIG_DATA
```

### 2. Télécharger le dataset
Télécharge `Reviews.csv` depuis Kaggle :
🔗 https://www.kaggle.com/snap/amazon-fine-food-reviews

Place-le dans `data/Reviews.csv`

### 3. Créer et activer l'environnement virtuel
```bash
python -m venv venv
venv\Scripts\activate
pip install pyspark==3.4.0 numpy pandas setuptools python-dotenv
```

### 4. Configurer Java 8 (OBLIGATOIRE avant chaque lancement)
```cmd
set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-8.0.482.8-hotspot
set PATH=%JAVA_HOME%\bin;%PATH%
set JAVA_TOOL_OPTIONS=
```

### 5. Lancer l'étape 1 — Nettoyage des données
```cmd
python etape1_nettoyage.py
```
**Résultat :** `data/cleaned_reviews.parquet` créé

### 6. Lancer l'étape 2 — Entraînement ALS
```cmd
python etape2_als_training.py
```
⏳ Durée : 10-20 minutes

**Résultat :** `als_model/` et `resultats_rmse.txt` créés

### 7. Lancer Kafka (Membre A doit avoir lancé docker-compose up -d)
```cmd
cd kafka
python producer.py
```

### 8. Lancer l'étape 3 — Streaming temps réel
Dans un nouveau terminal :
```cmd
venv\Scripts\activate
set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-8.0.482.8-hotspot
set PATH=%JAVA_HOME%\bin;%PATH%
set JAVA_TOOL_OPTIONS=
python etape3_spark_streaming.py
```

**Résultat :** Recommandations Top-5 générées dans `output/recommendations/`

---

## 📊 Exemple de recommandations générées

```
+--------+-----------------------------------------------------------------------+
|user_idx|recommendations                                                        |
+--------+-----------------------------------------------------------------------+
|8022    |[{3924, 8.76}, {3140, 5.83}, {3979, 5.18}, {617, 5.08}, {5586, 5.04}] |
|9692    |[{3924, 5.91}, {5038, 4.97}, {617, 4.93}, {5939, 4.88}, {695, 4.80}]  |
+--------+-----------------------------------------------------------------------+
```

---


# 👤 Partie C — Ingénieur Pipeline / Airflow
## Système de Recommandation Big Data 

---

## 🎯 Ce que fait cette partie

Ce module **orchestre automatiquement** tout le pipeline via Apache Airflow :
1. Vérifie que tous les fichiers des Membres A et B sont en place
2. Lance le producteur Kafka (ingestion des données)
3. Lance Spark Streaming + ALS (génération des recommandations Top-5)
4. Monitore le pipeline et génère un rapport JSON

**Interface web Airflow :** http://localhost:8081 (admin / admin)

---

## ✅ Résultats obtenus

| Métrique | Valeur |
|---|---|
| Tâches orchestrées | 5 (verifier → ecrire_scripts → kafka → spark → monitoring) |
| Fichiers JSON générés | 24 fichiers de recommandations |
| RMSE test final | 0.8334 (modèle Membre B) |
| Statut pipeline | SUCCES |
| Broker Kafka Docker | kafka:9093 |

---

## 🗂️ Structure des fichiers

```
projet-recommandation_BIG_DATA/
├── docker-compose.yml                         ← Kafka avec double listener (PC + Docker)
└── airflow/
    ├── Dockerfile                             ← Image Airflow avec Java 11 + PySpark
    ├── docker-compose-airflow.yml             ← Lance Airflow dans Docker
    └── dags/
        └── recommendation_pipeline_dag.py     ← Le DAG principal (5 tâches)
```

---

## ⚙️ Prérequis

| Outil | Version |
|---|---|
| Docker Desktop | Dernière version |
| Python | 3.10+ |
| Git | Dernière version |

> **Note :** Java 11 (Temurin) est installé automatiquement dans le conteneur Docker Airflow via le Dockerfile. Java 8 n'est requis que pour lancer les scripts directement sur Windows (Membres A et B).

---

## 🚀 Installation et lancement (étape par étape)

### Prérequis — Avoir fait les étapes des Membres A et B

Avant de lancer Airflow, s'assurer que :
- `data/Reviews.csv` est présent (Membre A)
- `als_model/` est présent sur GitHub (Membre B)
- `data/cleaned_reviews.parquet` est généré (lancer `python etape1_nettoyage.py`)

### 1. Cloner le repo

```bash
git clone https://github.com/salmaaz29/projet-recommandation_BIG_DATA.git
cd projet-recommandation_BIG_DATA
```

### 2. Générer les données nettoyées (si pas déjà fait)

```cmd
set JAVA_HOME=C:\Program Files\OpenLogic\jdk-8.0.442.06-hotspot
set PATH=%JAVA_HOME%\bin;%PATH%
set JAVA_TOOL_OPTIONS=
venv\Scripts\activate
python etape1_nettoyage.py
```

### 3. Créer les dossiers Airflow

```cmd
mkdir airflow\logs
mkdir airflow\plugins
mkdir checkpoints\streaming
```

### 4. Lancer Kafka (double listener PC + Docker)

```cmd
docker-compose up -d
```

Vérifier :
```cmd
docker ps
```
Tu dois voir : `kafka` et `zookeeper`

### 5. Construire l'image Airflow avec Java 11

```cmd
cd airflow
docker-compose -f docker-compose-airflow.yml build
```
⏳ Durée : 5-10 minutes

### 6. Initialiser Airflow

```cmd
docker-compose -f docker-compose-airflow.yml up airflow-init
```

Attendre : `admin user created successfully`

### 7. Lancer Airflow

```cmd
docker-compose -f docker-compose-airflow.yml up -d airflow-webserver airflow-scheduler
```

### 8. Ouvrir l'interface web

🔗 **http://localhost:8081**

Login : `admin` / Mot de passe : `admin`

### 9. Déclencher le DAG

1. Chercher le DAG : **`recommendation_pipeline`**
2. Activer le toggle (bouton à gauche)
3. Cliquer ▶️ **Trigger DAG**

Les 5 tâches vont s'exécuter dans l'ordre :

```
verifier_prerequis → ecrire_scripts → start_kafka_producer → generate_recommendations → monitoring_pipeline
```

---

## 📊 Résultats attendus

Après l'exécution complète du DAG :

```
output/
├── recommendations/     ← Fichiers JSON avec recommandations Top-5
└── rapport_monitoring.json  ← Rapport du pipeline
```

Exemple de rapport :
```json
{
  "pipeline": "Kafka -> Spark ALS -> Recommandations",
  "modele": {
    "algorithme": "ALS (Alternating Least Squares)",
    "rmse_validation": 0.8114,
    "rmse_test": 0.8334,
    "config": {"rank": 20, "regParam": 0.2, "maxIter": 15}
  },
  "fichiers_generes": 24,
  "statut": "SUCCES"
}
```

---

## 🏗️ Architecture réseau Docker

```
PC Windows
├── localhost:9092  ← Kafka pour les scripts Python (Membre A, B)
├── localhost:8081  ← Interface Airflow
└── Docker Network
    ├── kafka:9093      ← Kafka pour les conteneurs (Airflow → Kafka)
    ├── airflow_scheduler
    └── airflow_webserver
```

---

# Dashboard


---

## 🎯 Ce que fait cette partie

Ce module **expose les résultats** du pipeline Big Data via :
- Une **API REST Flask** pour servir les recommandations
- Un **Dashboard web interactif** pour visualiser les résultats

L'utilisateur tape un `user_idx`, l'API interroge les recommandations générées par Spark (Personne 2), et le dashboard affiche les produits Top-N.

**Interface :** http://localhost:5000

---

## ✅ Résultats attendus

| Fonctionnalité | Statut |
|---|---|
| GET /api/health | ✅ Vérification API |
| GET /api/recommendations/user/{id} | ✅ Recommandations Top-N |
| GET /api/users | ✅ Liste utilisateurs disponibles |
| GET /api/stats | ✅ RMSE + config modèle |
| GET /api/random-user | ✅ User aléatoire |
| Dashboard interactif | ✅ Interface web complète |

---

## 🗂️ Structure des fichiers

```
projet-recommandation_BIG_DATA/
├── api/
│   ├── app.py               ← API Flask principale
│   ├── requirements.txt     ← flask, flask-cors
│   ├── Dockerfile           ← Image Docker de l'API
│   ├── test_api.py          ← Tests de l'API
│   └── templates/
│       └── dashboard.html   ← Dashboard web interactif
└── docker-compose-api.yml   ← Lance l'API en Docker
```

---

## ⚙️ Prérequis

| Outil | Version |
|---|---|
| Python | 3.10+ |
| Docker Desktop | Dernière version |

> **Dépendances  :** Le pipeline des parties  2 et 3 doit avoir tourné pour avoir des données dans `output/recommendations/`.

---

## 🚀 Installation et lancement

### Option A — Sans Docker (développement local)

```bash
# 1. Aller dans le dossier api/
cd api

# 2. Installer les dépendances
pip install -r requirements.txt

# 3. Lancer l'API
python app.py
```

Ouvrir http://localhost:5000

---

### Option B — Avec Docker

```bash
# Depuis la racine du projet
docker-compose -f docker-compose-api.yml up -d --build

# Vérifier que le conteneur tourne
docker ps
```

Ouvrir http://localhost:5000

---

## 🔌 Endpoints de l'API

### `GET /api/health`
Vérifie que l'API est opérationnelle.
```json
{
  "status": "ok",
  "utilisateurs_en_cache": 24,
  "fichiers_trouves": 3
}
```

---

### `GET /api/recommendations/user/{user_id}`
Retourne les recommandations Top-N pour un utilisateur.

**Paramètre optionnel :** `?n=5` (1-20, défaut=5)

```bash
GET /api/recommendations/user/101?n=5
```

```json
{
  "user_id": 101,
  "recommendations": ["Product_45", "Product_78", "Product_19", "Product_3", "Product_22"],
  "details": [
    {"product_id": "Product_45", "product_idx": 45, "score": 8.7634},
    {"product_id": "Product_78", "product_idx": 78, "score": 7.2100}
  ],
  "total": 5
}
```

---

### `GET /api/users`
Liste tous les `user_idx` disponibles.

```json
{
  "total": 48,
  "user_ids": [101, 230, 445, ...]
}
```

---

### `GET /api/stats`
Statistiques du modèle ALS et du pipeline.

```json
{
  "total_utilisateurs_avec_recommandations": 48,
  "modele": {
    "algorithme": "ALS",
    "rmse_validation": 0.8114,
    "rmse_test": 0.8334,
    "config": {"rank": 20, "regParam": 0.2, "maxIter": 15}
  }
}
```

---

### `GET /api/random-user`
Retourne un `user_idx` aléatoire parmi ceux disponibles.

```json
{"user_idx": 8022}
```

---

### `GET /api/recommendations/all?limit=20`
Aperçu de toutes les recommandations.

---

## 🧪 Lancer les tests

```bash
# Assurez-vous que l'API tourne d'abord (python app.py)
cd api
python test_api.py
```

Sortie attendue :
```
✅ API opérationnelle
✅ Stats OK
✅ Users OK
✅ Erreur 404 correctement gérée
✅ user_idx aléatoire : 8022
✅ TOUS LES TESTS SONT PASSÉS
```

---

## ❗ Problèmes fréquents et solutions

| Erreur | Solution |
|---|---|
| `No module named flask` | `pip install flask flask-cors` |
| `Aucune recommandation disponible` | Lancer d'abord `etape3_spark_streaming.py` |
| Port 5000 déjà utilisé | Modifier `app.run(port=5001)` dans app.py |
| Données JSON vides | Vérifier que `output/recommendations/*.json` existe |
| Dashboard blanc | Vérifier que `templates/dashboard.html` est dans `api/templates/` |

---

