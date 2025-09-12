import os 
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
import requests

# Chargement des variables d'environnement
load_dotenv()
url = os.getenv("GTFS_RT_VP_URL")

if not url:
    raise ValueError("⚠️ Variable d'environnement GTFS_RT_TU_URL non définie.")
# Télechargement des données
response = requests.get(url, timeout=30)

# Parser le flux 
feed = gtfs_realtime_pb2.FeedMessage()
feed.ParseFromString(response.content)

# Création du dossier d'export
os.makedirs("exports", exist_ok = True)
out_path = os.path.join("exports", "vehicle_positions.txt")

# Écriture des données
with open(out_path, "w", encoding="utf-8") as f:
    for entity in feed.entity:
        if entity.HasField("vehicle"):
            f.write(str(entity.vehicle))
            f.write("\n") # pour séparer les entités entre eux

print(f"✅ Exportation terminée : {out_path}")