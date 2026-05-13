"""
Personne 4 — API Flask + Dashboard
Système de Recommandation de Produits en Temps Réel
"""

from flask import Flask, jsonify, request, render_template_string
from flask_cors import CORS
import json
import os
import glob
import random

app = Flask(__name__)
CORS(app)  # Autorise les requêtes cross-origin depuis le dashboard

# ── Chemins vers les outputs de Personne 2 & 3 ─────────────────────
PROJECT_ROOT         = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
RECOMMENDATIONS_DIR  = os.path.join(PROJECT_ROOT, 'output', 'recommendations')
RAPPORT_PATH         = os.path.join(PROJECT_ROOT, 'output', 'rapport_monitoring.json')
RMSE_PATH            = os.path.join(PROJECT_ROOT, 'resultats_rmse.txt')


# ══════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════

def load_all_recommendations():
    """Charge tous les fichiers JSON de recommandations générés par Spark."""
    recs = {}
    patterns = [
        os.path.join(RECOMMENDATIONS_DIR, '**', '*.json'),
        os.path.join(RECOMMENDATIONS_DIR, '*.json'),
    ]
    for pattern in patterns:
        for filepath in glob.glob(pattern, recursive=True):
            try:
                with open(filepath, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        data = json.loads(line)
                        user_idx = data.get('user_idx')
                        if user_idx is not None:
                            recs[str(user_idx)] = data.get('recommendations', [])
            except Exception as e:
                print(f"Erreur lecture {filepath}: {e}")
    return recs


def load_rapport():
    """Charge le rapport de monitoring généré par Airflow."""
    if os.path.exists(RAPPORT_PATH):
        with open(RAPPORT_PATH, 'r') as f:
            return json.load(f)
    return None


def load_rmse():
    """Charge les résultats RMSE."""
    if os.path.exists(RMSE_PATH):
        with open(RMSE_PATH, 'r') as f:
            return f.read()
    return "Fichier resultats_rmse.txt non trouvé"


# ══════════════════════════════════════════════════════════════
#  ROUTES API
# ══════════════════════════════════════════════════════════════

@app.route('/api/health', methods=['GET'])
def health():
    """Vérifie que l'API tourne."""
    recs = load_all_recommendations()
    return jsonify({
        "status": "ok",
        "message": "API de recommandation opérationnelle",
        "utilisateurs_en_cache": len(recs),
        "dossier_recommandations": RECOMMENDATIONS_DIR,
        "fichiers_trouves": len(glob.glob(os.path.join(RECOMMENDATIONS_DIR, '**', '*.json'), recursive=True))
    })


@app.route('/api/recommendations/user/<user_id>', methods=['GET'])
def get_recommendations(user_id):
    """
    Retourne les Top-N recommandations pour un user_idx donné.
    Paramètre optionnel : ?n=5 (nombre de recommandations, défaut=5)

    Exemple : GET /api/recommendations/user/101
    """
    n = request.args.get('n', 5, type=int)
    n = min(max(n, 1), 20)  # Clamp entre 1 et 20

    recs = load_all_recommendations()
    user_key = str(user_id)

    if user_key not in recs:
        # Tentative avec user_idx voisins (±500) si l'utilisateur exact n'existe pas
        available_keys = list(recs.keys())
        if not available_keys:
            return jsonify({
                "error": "Aucune recommandation disponible. Lancez d'abord le pipeline Kafka → Spark.",
                "user_id": user_id,
                "hint": "Assurez-vous que etape3_spark_streaming.py a été exécuté."
            }), 404

        return jsonify({
            "error": f"Utilisateur {user_id} non trouvé dans les recommandations.",
            "user_id": user_id,
            "hint": f"Essayez un de ces user_idx disponibles : {available_keys[:10]}",
            "total_utilisateurs": len(available_keys)
        }), 404

    user_recs = recs[user_key][:n]

    # Formater la réponse (compatible avec le format Spark ALS)
    products = []
    for rec in user_recs:
        if isinstance(rec, dict):
            products.append({
                "product_id":  f"Product_{rec.get('product_idx', rec.get('0', '?'))}",
                "product_idx": rec.get('product_idx', rec.get('0', '?')),
                "score":       round(float(rec.get('rating', rec.get('1', 0))), 4)
            })
        elif isinstance(rec, list) and len(rec) >= 2:
            products.append({
                "product_id":  f"Product_{rec[0]}",
                "product_idx": rec[0],
                "score":       round(float(rec[1]), 4)
            })

    return jsonify({
        "user_id":         int(user_id),
        "recommendations": [p["product_id"] for p in products],
        "details":         products,
        "total":           len(products)
    })


@app.route('/api/recommendations/all', methods=['GET'])
def get_all_recommendations():
    """Retourne un résumé de toutes les recommandations disponibles."""
    recs = load_all_recommendations()
    limit = request.args.get('limit', 20, type=int)

    sample = []
    for user_idx, user_recs in list(recs.items())[:limit]:
        products = []
        for rec in user_recs[:5]:
            if isinstance(rec, dict):
                products.append(f"Product_{rec.get('product_idx', '?')}")
            elif isinstance(rec, list):
                products.append(f"Product_{rec[0]}")
        sample.append({"user_idx": user_idx, "top5": products})

    return jsonify({
        "total_utilisateurs": len(recs),
        "sample":             sample
    })


@app.route('/api/users', methods=['GET'])
def get_available_users():
    """Liste les user_idx disponibles (pour le dashboard)."""
    recs = load_all_recommendations()
    users = sorted([int(k) for k in recs.keys()])
    return jsonify({
        "total":   len(users),
        "user_ids": users[:200]  # Limité à 200 pour la réponse
    })


@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Statistiques du pipeline (RMSE, config modèle, etc.)."""
    rapport = load_rapport()
    rmse_txt = load_rmse()
    recs = load_all_recommendations()

    stats = {
        "total_utilisateurs_avec_recommandations": len(recs),
        "rmse_info": rmse_txt,
        "rapport_pipeline": rapport,
    }

    if rapport:
        stats["modele"] = rapport.get("modele", {})
        stats["statut_pipeline"] = rapport.get("statut", "INCONNU")

    return jsonify(stats)


@app.route('/api/random-user', methods=['GET'])
def random_user():
    """Retourne un user_idx aléatoire (pour démo)."""
    recs = load_all_recommendations()
    if not recs:
        return jsonify({"error": "Aucune recommandation disponible"}), 404
    user_idx = random.choice(list(recs.keys()))
    return jsonify({"user_idx": int(user_idx)})


# ══════════════════════════════════════════════════════════════
#  ROUTE PRINCIPALE : Dashboard HTML (servi par Flask)
# ══════════════════════════════════════════════════════════════

@app.route('/')
def dashboard():
    """Sert le dashboard HTML principal."""
    dashboard_path = os.path.join(os.path.dirname(__file__), 'templates', 'dashboard.html')
    if os.path.exists(dashboard_path):
        with open(dashboard_path, 'r', encoding='utf-8') as f:
            return f.read()
    return "<h1>Dashboard non trouvé. Placez dashboard.html dans api/templates/</h1>", 404


# ══════════════════════════════════════════════════════════════
#  LANCEMENT
# ══════════════════════════════════════════════════════════════

if __name__ == '__main__':
    print("=" * 60)
    print("  API Recommandation — ")
    print("  http://localhost:5000")
    print("  http://localhost:5000/api/health")
    print("  http://localhost:5000/api/recommendations/user/101")
    print("=" * 60)
    app.run(host='0.0.0.0', port=5000, debug=True)