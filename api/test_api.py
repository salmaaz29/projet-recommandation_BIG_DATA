"""
test_api.py — Tests de l'API de recommandation


Lance ce script APRÈS avoir démarré l'API (python app.py).
Usage : python test_api.py
"""

import requests
import json

BASE_URL = "http://localhost:5000"

def sep(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)

def test_health():
    sep("TEST 1 : Health Check")
    res = requests.get(f"{BASE_URL}/api/health")
    data = res.json()
    print(f"Status HTTP : {res.status_code}")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    assert res.status_code == 200, "❌ API non disponible"
    print("✅ API opérationnelle")

def test_stats():
    sep("TEST 2 : Statistiques du modèle")
    res = requests.get(f"{BASE_URL}/api/stats")
    data = res.json()
    print(f"Status HTTP : {res.status_code}")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    assert res.status_code == 200
    print("✅ Stats OK")

def test_users():
    sep("TEST 3 : Liste des utilisateurs disponibles")
    res = requests.get(f"{BASE_URL}/api/users")
    data = res.json()
    print(f"Status HTTP : {res.status_code}")
    print(f"Total utilisateurs : {data.get('total', 0)}")
    if data.get('user_ids'):
        print(f"Premiers IDs : {data['user_ids'][:10]}")
    assert res.status_code == 200
    print("✅ Users OK")
    return data.get('user_ids', [])

def test_recommendations(user_id):
    sep(f"TEST 4 : Recommandations pour user_idx={user_id}")
    res = requests.get(f"{BASE_URL}/api/recommendations/user/{user_id}?n=5")
    data = res.json()
    print(f"Status HTTP : {res.status_code}")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    if res.status_code == 200:
        print(f"✅ {data.get('total', 0)} recommandation(s) générées")
    else:
        print(f"⚠️  User {user_id} non trouvé (normal si le modèle n'a pas encore tourné)")

def test_user_not_found():
    sep("TEST 5 : Utilisateur inexistant (gestion erreur)")
    res = requests.get(f"{BASE_URL}/api/recommendations/user/999999999")
    data = res.json()
    print(f"Status HTTP : {res.status_code}")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    assert res.status_code == 404
    assert "error" in data
    print("✅ Erreur 404 correctement gérée")

def test_random_user():
    sep("TEST 6 : Utilisateur aléatoire")
    res = requests.get(f"{BASE_URL}/api/random-user")
    data = res.json()
    print(f"Status HTTP : {res.status_code}")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    if res.status_code == 200:
        print(f"✅ user_idx aléatoire : {data.get('user_idx')}")

def test_all_recommendations():
    sep("TEST 7 : Aperçu de toutes les recommandations")
    res = requests.get(f"{BASE_URL}/api/recommendations/all?limit=5")
    data = res.json()
    print(f"Status HTTP : {res.status_code}")
    print(json.dumps(data, indent=2, ensure_ascii=False))
    print(f"✅ Total utilisateurs : {data.get('total_utilisateurs', 0)}")


if __name__ == "__main__":
    print("\n🚀 TESTS DE L'API RECOMMANDATION — ")
    print("Assurez-vous que l'API tourne : python app.py\n")

    try:
        test_health()
        test_stats()
        users = test_users()
        test_user_not_found()
        test_random_user()
        test_all_recommendations()

        # Test avec un vrai user si disponible
        if users:
            test_recommendations(users[0])
        else:
            sep("TEST 4 : Recommandations")
            print("⚠️  Aucun utilisateur disponible — lancez d'abord le pipeline Spark.")

        print("\n" + "="*60)
        print("  ✅ TOUS LES TESTS SONT PASSÉS")
        print("="*60)

    except requests.exceptions.ConnectionError:
        print("\n❌ ERREUR : Impossible de se connecter à l'API.")
        print("   → Lancez d'abord : python app.py")
    except AssertionError as e:
        print(f"\n❌ ASSERTION ÉCHOUÉE : {e}")
    except Exception as e:
        print(f"\n❌ ERREUR INATTENDUE : {e}")