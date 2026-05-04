# 📦 Membre A — Kafka + Docker Compose
> Ingestion des données en temps réel | Projet Big Data 2025-2026

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

## 🔌 Informations pour les autres membres

| Paramètre | Valeur |
|-----------|--------|
| Broker Kafka | `localhost:9092` |
| Nom du topic | `reviews_stream` |
| Format des messages | `{UserId, ProductId, Score, Time}` |
| Nombre de messages | 568 454 |
| Délai entre messages | 0.1 seconde |

### Pour Membre B (Spark) :
Connecte Spark Streaming au broker `localhost:9092` et au topic `reviews_stream`.  
Le producer doit être lancé **avant** ton job Spark Streaming.

### Pour Membre C (Airflow) :
Le script à appeler dans ton DAG est `kafka/producer.py`.  
Assure-toi que `docker-compose up -d` est lancé avant de déclencher le DAG.

### Pour Membre D (API/Dashboard) :
Le producer tourne en continu. Tu peux consommer les messages directement  
depuis `localhost:9092` / topic `reviews_stream` si tu veux afficher le flux en direct.

---

## 🛑 Commandes utiles

| Action | Commande |
|--------|----------|
| Démarrer Kafka | `docker-compose up -d` |
| Arrêter Kafka | `docker-compose down` |
| Voir les logs Kafka | `docker logs kafka` |
| Voir les logs Zookeeper | `docker logs zookeeper` |
| Lister les topics | `docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list` |
| Vérifier les messages | `docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic reviews_stream --from-beginning` |

---

## ❗ Problèmes fréquents

**Erreur : `open //./pipe/dockerDesktopLinuxEngine`**  
→ Docker Desktop n'est pas démarré. Ouvre-le et attends que l'icône soit verte.

**Erreur : `kafka.errors.NoBrokersAvailable`**  
→ Kafka n'est pas lancé. Lance `docker-compose up -d` d'abord, puis relance `producer.py`.

**Erreur : `FileNotFoundError: Reviews.csv`**  
→ Le fichier CSV n'est pas dans le bon dossier. Vérifie qu'il est bien dans `data/Reviews.csv`.

**Warning : `version is obsolete`**  
→ Simple avertissement, pas une erreur. Peut être ignoré sans problème.

---

## 👤 Auteur

**Membre A** — Ingestion des données / Infrastructure Kafka  
Module Big Data — 2025-2026


# 👤 Membre B — Spark MLlib + ALS (Data Scientist)
## Système de Recommandation Big Data 2025-2026

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

## 🔌 Informations pour les autres membres

### Pour Membre C (Airflow) :
| Paramètre | Valeur |
|---|---|
| Script étape 1 | `python etape1_nettoyage.py` |
| Script étape 2 | `python etape2_als_training.py` |
| Script étape 3 | `python etape3_spark_streaming.py` |
| Dépendance étape 1 | Aucune — peut tourner seul |
| Dépendance étape 2 | Après étape 1 |
| Dépendance étape 3 | Après Kafka (Membre A) + étape 2 |
| Java requis | `set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-8.0.482.8-hotspot` |

### Pour Membre D (API/Dashboard) :
| Paramètre | Valeur |
|---|---|
| Modèle ALS | `als_model/` |
| Recommandations JSON | `output/recommendations/` |
| Format recommandations | `{user_idx, recommendations: [{product_idx, score}]}` |
| Mise à jour | Toutes les 10 secondes (streaming) |

---

## ❗ Problèmes fréquents et solutions

| Erreur | Solution |
|---|---|
| `getSubject is not supported` | Utiliser Java 8 : `set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-8.0.482.8-hotspot` |
| `No module named distutils` | `pip install setuptools` |
| `Ratings MUST NOT be Null or NaN` | Ajouter `df.dropna()` avant `als.fit()` |
| `kafka:9092 DNS resolution failed` | Utiliser `localhost:9092` dans le script streaming |
| `No module named pyspark` | Activer le venv : `venv\Scripts\activate` |

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

## 👤 Auteur
Membre B — Data Scientist / Spark MLlib + ALS  
Module Big Data — 2025-2026