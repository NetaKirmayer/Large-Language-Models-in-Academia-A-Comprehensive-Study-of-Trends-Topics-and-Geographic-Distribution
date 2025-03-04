import requests
import pandas as pd
from time import sleep
import os
import json

url = "https://api.openalex.org/institutions"

# CSV file name
csv_filename = "openalex_institutions_results.csv"
cursor_filename = "openalex_institutions_cursor.json"

if os.path.exists(cursor_filename):
    with open(cursor_filename, "r") as f:
        cursor = json.load(f).get("cursor", "*")
else:
    cursor = "*"

per_page = 200  
max_retries = 5  

# Open CSV file and write header if new file
if not os.path.exists(csv_filename):
    with open(csv_filename, "w", encoding="utf-8") as csv_file:
        csv_file.write("id,display_name,country_code,latitude,longitude,type\n")

while cursor:
    print(f"Fetching page with cursor: {cursor}")

    for attempt in range(max_retries):
        try:
            params = {
                "per_page": per_page,
                "cursor": cursor
            }
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()  
            break  
        except requests.exceptions.RequestException as e:
            print(f"Request failed (attempt {attempt+1}/{max_retries}): {e}")
            sleep(5 * (2 ** attempt)) 
    else:
        print("Max retries reached, skipping this cursor.")
        break

    data = response.json()
    institutions = data.get("results", [])

    filtered_institutions = [
        {
            "id": inst.get("id"),
            "display_name": inst.get("display_name"),
            "country_code": inst.get("country_code"),
            "latitude": inst.get("geo", {}).get("latitude"),
            "longitude": inst.get("geo", {}).get("longitude"),
            "type": inst.get("type")
        }
        for inst in institutions
    ]

    df = pd.DataFrame(filtered_institutions)
    if not df.empty:
        df.to_csv(csv_filename, mode='a', index=False, header=False)

    cursor = data.get("meta", {}).get("next_cursor", None)
    with open(cursor_filename, "w") as f:
        json.dump({"cursor": cursor}, f)

    sleep(3) 
print(f"Saved institutions to {csv_filename}")