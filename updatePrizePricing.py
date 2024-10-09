import psycopg2
from psycopg2.extras import execute_batch
import os
from dotenv import load_dotenv
from urllib.parse import urlparse
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

def query_prize_table():

    load_dotenv()
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("DATABASE_URL not found in .env file")
        return

    # Parse the DATABASE_URL
    parsed_url = urlparse(database_url)
    dbname = parsed_url.path[1:]  
    user = parsed_url.username
    password = parsed_url.password
    host = parsed_url.hostname
    port = parsed_url.port or 5432 

    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
            sslmode='require'
        )
        print("Connected to the database successfully!")

        cur = conn.cursor()

        cur.execute("SELECT purple_mana_new_inv_id, id FROM prize WHERE is_manually_priced = false")

        rows = cur.fetchall()

        ids = [(row[0], row[1]) for row in rows]

        return ids

    except psycopg2.Error as e:
        print("Error connecting to the database or querying data:")
        print(e)
        return []

    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()
        print("Database connection closed.")

def make_api_request(purple_mana_id, database_id):
    base_url = os.getenv('PURPLEMANA_API_URL')
    
    numeric_id = purple_mana_id.split('-')[0]

    input_param = f"%7B%220%22%3A%7B%22json%22%3A%7B%22id%22%3A%22{numeric_id}%22%7D%7D%2C%221%22%3A%7B%22json%22%3A%7B%22product_id%22%3A{numeric_id}%7D%7D%7D"
    full_url = f"{base_url}?batch=1&input={input_param}"
    
    response = requests.get(full_url)
    
    if response.status_code == 200:
        data = response.json()
        processed_data = {
            "purple_mana_id": purple_mana_id,
            "tcglow": data[0]["result"]["data"]["json"].get("pricing_today", {}).get("tcglow"),
        }
        return database_id, processed_data
    else:
        return database_id, f"Error: {response.status_code} - {response.text}"

def update_prize_table(results):
    load_dotenv()
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("DATABASE_URL not found in .env file")
        return

    # Parse the DATABASE_URL
    parsed_url = urlparse(database_url)
    dbname = parsed_url.path[1:] 
    user = parsed_url.username
    password = parsed_url.password
    host = parsed_url.hostname
    port = parsed_url.port or 5432 

    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port,
            sslmode='require'
        )
        print("Connected to the database successfully!")

        cur = conn.cursor()

        # Prepare the data for batch update
        update_data = []
        for database_id, data in results.items():
            if 'tcglow' in data and isinstance(data['tcglow'], dict):
                condition = data['purple_mana_id'].split('-', 1)[1].replace('-', ' ').title()
                price = data['tcglow'].get(condition)
                if price is not None:
                    update_data.append((price, database_id))

        # Perform batch update
        execute_batch(cur, 
                      "UPDATE prize SET value = %s WHERE id = %s",
                      update_data)

        conn.commit()
        print(f"Updated {len(update_data)} rows in the prize table.")

    except psycopg2.Error as e:
        print("Error connecting to the database or updating data:")
        print(e)

    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()
        print("Database connection closed.")

def main():
    ids = query_prize_table()
    results = {}
    
    with ThreadPoolExecutor(max_workers=16) as executor:
        future_to_id = {executor.submit(make_api_request, purple_mana_id, database_id): database_id for purple_mana_id, database_id in ids}
        for future in as_completed(future_to_id):
            try:
                database_id, result = future.result()
                if "error" in result:
                    print(f"Error processing database_id {database_id}: {result['error']}")
                results[database_id] = result
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
    
    print(f"Processed {len(results)} items:")
    print(json.dumps(results, indent=2))

    # Update the prize table with the new values
    update_prize_table(results)

if __name__ == "__main__":
    main()