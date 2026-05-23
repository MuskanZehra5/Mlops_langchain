# Databricks notebook source
import time
import random
import json
import threading
from datetime import datetime, timedelta

# Make students complete these paths
volume_path = "/Volumes/24280060_pa2/bronze/temp"
flight_landing_zone = f"{volume_path}/flight_landing_zone_part2"
cdc_landing_zone = f"{volume_path}/cdc_flight_landing_zone_part2"

dbutils.fs.mkdirs(flight_landing_zone)
dbutils.fs.mkdirs(cdc_landing_zone)

# === DO NOT CHANGE ===
IATA_codes = ["LHE", "DXB", "DOH", "RUH", "AUH", "KWI", "JED"]
distance_map = {
    'LHE-DXB': 1233, 'LHE-DOH': 1447, 'LHE-RUH': 1741, 'LHE-AUH': 1298, 'LHE-KWI': 1579, 'LHE-JED': 2269, 'DXB-LHE': 1233, 
    'DXB-DOH': 234, 'DXB-RUH': 542, 'DXB-AUH': 72, 'DXB-KWI': 530, 'DXB-JED': 1055, 'DOH-LHE': 1447, 'DOH-DXB': 234, 
    'DOH-RUH': 307, 'DOH-AUH': 199, 'DOH-KWI': 352, 'DOH-JED': 826, 'RUH-LHE': 1741, 'RUH-DXB': 542, 'RUH-DOH': 307, 
    'RUH-AUH': 500, 'RUH-KWI': 305, 'RUH-JED': 529, 'AUH-LHE': 1298, 'AUH-DXB': 72, 'AUH-DOH': 199, 'AUH-RUH': 500, 
    'AUH-KWI': 528, 'AUH-JED': 1002, 'KWI-LHE': 1579, 'KWI-DXB': 530, 'KWI-DOH': 352, 'KWI-RUH': 305, 'KWI-AUH': 528, 
    'KWI-JED': 757, 'JED-LHE': 2269, 'JED-DXB': 1055, 'JED-DOH': 826, 'JED-RUH': 529, 'JED-AUH': 1002, 'JED-KWI': 757
}
location_lookup = {
    "LHE": "Lahore, PB", "DXB": "Dubai, DU", "DOH": "Doha, QA",
    "RUH": "Riyadh, RI", "AUH": "Abu Dhabi, AZ", "KWI": "Kuwait City, KW",
    "JED": "Jeddah, MK"
}

# COMMAND ----------

# === DO NOT CHANGE ===
def generate_flight_data(seed=0):
    random.seed(seed)

    # File Structure
    rows = ["date,delay,distance,origin,destination"]

    # Random Date
    base_time = datetime(2025, random.randint(1, 12), random.randint(1, 28), random.randint(0, 23), random.randint(0, 59))
    
    # Random number of rows per file
    num_rows = random.randint(10, 50)
    
    # File Row Generator
    for row_idx in range(num_rows):
        origin = random.choice(IATA_codes)
        dest = random.choice([c for c in IATA_codes if c != origin])
        route = f"{origin}-{dest}"
        dist = distance_map.get(route, random.randint(300, 2000))
        current_row_time = base_time + timedelta(minutes=row_idx)
        rows.append(f"{current_row_time.strftime('%m%d%H%M')},{random.randint(-15, 300)},{dist},{origin},{dest}")

    return "\n".join(rows)

# === DO NOT CHANGE ===
def generate_cdc_updates(seed=0, seed_hist=0):
    random.seed(seed)
    data_pool = []

    # Random Date
    base_time = datetime(2025, random.randint(1, 12), random.randint(1, 28), random.randint(0, 23), random.randint(0, 59))

    # Random number of rows per file
    num_rows = random.randint(10, 50)
    
    # File Row Generator
    for row_idx in range(num_rows):
        origin = random.choice(IATA_codes)
        dest = random.choice([c for c in IATA_codes if c != origin])
        standardized_route = "-".join(sorted([origin, dest]))
        
        action = "DELETE" if random.random() < 0.10 else "UPDATE"
        seq = 3
        
        if seed > 0 and seed == seed_hist:
            seq += random.randint(1, 3) if random.randint(1, 10) <= 8 else random.randint(-2, -1)
            
        event = {
            "update_ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "action": action,
            "seq": seq,
            "payload": {
                "date": (base_time + timedelta(minutes=row_idx)).strftime("%m%d%H%M"),
                "departure": location_lookup.get(origin),
                "arrival": location_lookup.get(dest),
                "standardized_route": standardized_route,
                "distance": distance_map.get(standardized_route, random.randint(300, 2000)),
                "delay": random.randint(-15, 300)
            }
        }
        data_pool.append(event)

    random.seed(seed)
    random.shuffle(data_pool)
    return "\n".join([json.dumps(record) for record in data_pool])

# COMMAND ----------

import time


num_iterations = 5
interval = 5
seed_hist = -1
volume_path = "/Volumes/24280060_pa2/bronze/temp"

for i in range(num_iterations):
    # 1. Generate and save base flight data
    flight_content = generate_flight_data(seed=i)
    flight_path = f"{volume_path}/flight_landing_zone_part2/batch_{i}.csv"
    dbutils.fs.put(flight_path, flight_content, overwrite=True)
    
    # 2. Generate and save exact matching CDC data
    cdc_content = generate_cdc_updates(seed=i, seed_hist=seed_hist)
    cdc_path = f"{volume_path}/cdc_flight_landing_zone_part2/cdc_batch_{i}.json"
    dbutils.fs.put(cdc_path, cdc_content, overwrite=True)
    
    seed_hist = i
    print(f"Generated matching batch {i} at {datetime.now().strftime('%H:%M:%S')}")
    # time.sleep(interval)

# COMMAND ----------

