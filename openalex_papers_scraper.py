import subprocess
subprocess.run(["pip", "install", "pyalex"])


from pyalex import Works, Authors, Sources, Institutions, Topics, Publishers, Funders
import pyalex
from pyalex import config


pyalex.config.email = "kirmayer@bgu.post.ac.il"

config.max_retries = 3
config.retry_backoff_factor = 0.1
config.retry_http_codes = [429, 500, 503]


import requests
import time
import json
import csv
import os

QUERY_FILE = "search_prompts.json" 
BASE_URL = "https://api.openalex.org/works"
PER_PAGE = 200  
MAX_REQUESTS_PER_SECOND = 10 
PROGRESS_FILE = "progress1.json"
CSV_FILE = "openalex_results1.csv"
TIMEOUT = 10  
MAX_RETRIES = 3  

def load_queries():
    with open(QUERY_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return [entry["search_prompt"] for entry in data]

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"query_index": 0, "cursor": "*"}

def save_progress(query_index, cursor):
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"query_index": query_index, "cursor": cursor}, f)

import csv
import os

SELECTED_FIELDS = [
    "id", "display_name", "doi", "title", "publication_year", "publication_date",
    "topics", "primary_topic", "keywords", "cited_by_count", "related_works",
    "concepts", "abstract_inverted_index", "best_oa_location"
]

def save_to_csv(results):
    file_exists = os.path.exists(CSV_FILE)

    with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=SELECTED_FIELDS, quoting=csv.QUOTE_ALL)

        if not file_exists: 
            writer.writeheader()

        for result in results:
            row = {field: json.dumps(result.get(field, None), ensure_ascii=False) if isinstance(result.get(field), (dict, list)) else result.get(field, None) for field in SELECTED_FIELDS}
            writer.writerow(row)

def scrape_openalex():
    queries = load_queries()
    progress = load_progress()
    query_index = progress["query_index"]
    cursor = progress["cursor"]

    while query_index < len(queries):
        query = queries[query_index]
        print(f"Query {query_index + 1}/{len(queries)}: {query}, starting with cursor {cursor}")

        while cursor:
            url = f"{BASE_URL}?search=\"{query}\"&per_page={PER_PAGE}&cursor={cursor}&select=" + ",".join(SELECTED_FIELDS)

            for attempt in range(MAX_RETRIES):
                try:
                    response = requests.get(url, timeout=TIMEOUT)  # ⏳ Set timeout

                    if response.status_code == 429:  # Too Many Requests
                        print("Rate limit hit. Saving progress and stopping for today.")
                        save_progress(query_index, cursor)
                        return

                    elif response.status_code != 200:
                        print(f"Error {response.status_code}: {response.text}")
                        save_progress(query_index, cursor)
                        return

                    data = response.json()
                    if "results" in data and data["results"]:
                        save_to_csv(data["results"])

                    cursor = data.get("meta", {}).get("next_cursor")
                    break  # Exit retry loop if request was successful

                except requests.exceptions.Timeout:
                    print(f"Timeout error on attempt {attempt + 1}. Retrying...")
                    time.sleep(2)  # Wait before retrying

                except requests.exceptions.ConnectionError:
                    print(f"Connection error on attempt {attempt + 1}. Retrying...")
                    time.sleep(2)  # Wait before retrying

                except Exception as e:
                    print(f"Unexpected error: {e}")
                    save_progress(query_index, cursor)
                    return

            else:
                print(f"Failed after {MAX_RETRIES} attempts. Saving progress and stopping.")
                save_progress(query_index, cursor)
                return

            time.sleep(1 / MAX_REQUESTS_PER_SECOND)

        query_index += 1
        cursor = "*"

    save_progress(query_index, cursor)

scrape_openalex()