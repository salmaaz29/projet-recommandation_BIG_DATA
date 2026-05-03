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