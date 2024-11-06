import psycopg2
from psycopg2.extras import execute_batch
import os
from dotenv import load_dotenv
from urllib.parse import urlparse
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

def query_prize_table():
    load_dotenv()
    database_url = os.getenv('STAGING_DATABASE_URL')
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
    base_url = "https://www.purplemana.com/api/trpc/catalogProducts.getOne,catalogProducts.getSalesHistory"
    
    # Ensure purple_mana_id is a string and remove any decimal point
    purple_mana_id = str(purple_mana_id).split('.')[0]
    numeric_id = purple_mana_id.split('-')[0]

    input_param = f"%7B%220%22%3A%7B%22json%22%3A%7B%22id%22%3A%22{numeric_id}%22%7D%7D%2C%221%22%3A%7B%22json%22%3A%7B%22product_id%22%3A{numeric_id}%7D%7D%7D"
    full_url = f"{base_url}?batch=1&input={input_param}"
    
    try:
        response = requests.get(full_url)
        response.raise_for_status()
        
        data = response.json()
        
        # # Log the raw data received
        # print(f"Raw data for {purple_mana_id}: {json.dumps(data, indent=2)}")
        
        if isinstance(data, list) and len(data) > 0:
            json_data = data[0].get('result', {}).get('data', {}).get('json', {})
            if isinstance(json_data, dict):
                tcglow = json_data.get('tcglow', {})
                if isinstance(tcglow, dict):
                    processed_data = {
                        "purple_mana_id": purple_mana_id,
                        "tcglow": tcglow,
                    }
                    # print(f"Processed data for {purple_mana_id}: {json.dumps(processed_data, indent=2)}")
                    return database_id, processed_data
                else:
                    return database_id, {"error": f"Invalid tcglow structure: {tcglow}"}
            else:
                return database_id, {"error": f"Invalid json data structure: {json_data}"}
        else:
            return database_id, {"error": f"Invalid API response structure: {data}"}
    except requests.RequestException as e:
        return database_id, {"error": f"Request failed: {str(e)}"}
    except json.JSONDecodeError:
        return database_id, {"error": "Invalid JSON response"}
    except Exception as e:
        return database_id, {"error": f"Unexpected error: {str(e)}"}

def update_prize_table(results):
    load_dotenv()
    database_url = os.getenv('STAGING_DATABASE_URL')
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
                # Extract condition from purple_mana_id and capitalize each word
                condition = ' '.join(word.capitalize() for word in data['purple_mana_id'].split('-')[1:])
                price = data['tcglow'].get(condition)
                if price is not None:
                    update_data.append((price, database_id))
                else:
                    print(f"No price found for condition '{condition}' in item {data['purple_mana_id']}")
            else:
                print(f"Invalid data structure for item {database_id}")

        print(f"Prepared {len(update_data)} items for update")

        # Perform batch update
        if update_data:
            execute_batch(cur, 
                          "UPDATE prize SET value = %s WHERE id = %s",
                          update_data)
            conn.commit()
            updated_rows = len(update_data)
        else:
            updated_rows = 0

        print(f"Actually updated {updated_rows} rows")
        return updated_rows

    except psycopg2.Error as e:
        print("Error connecting to the database or updating data:")
        print(e)
        return 0  # Return 0 if there was an error

    finally:
        if 'cur' in locals() and cur:
            cur.close()
        if 'conn' in locals() and conn:
            conn.close()
        print("Database connection closed.")

def main():
    ids = query_prize_table()
    results = {}
    errors = []
    
    def process_batch(batch):
        batch_results = {}
        batch_errors = []
        with ThreadPoolExecutor(max_workers=32) as executor:
            future_to_id = {executor.submit(make_api_request, purple_mana_id, database_id): (purple_mana_id, database_id) for purple_mana_id, database_id in batch}
            for future in as_completed(future_to_id):
                purple_mana_id, database_id = future_to_id[future]
                try:
                    _, result = future.result()
                    if "error" in result:
                        batch_errors.append({
                            "purple_mana_id": purple_mana_id,
                            "database_id": database_id,
                            "error": result["error"]
                        })
                    else:
                        batch_results[database_id] = result
                except Exception as e:
                    batch_errors.append({
                        "purple_mana_id": purple_mana_id,
                        "database_id": database_id,
                        "error": str(e)
                    })
        return batch_results, batch_errors

    # First pass
    results, errors = process_batch(ids)

    # Retry failed requests
    if errors:
        print(f"Retrying {len(errors)} failed requests...")
        retry_ids = [(error['purple_mana_id'], error['database_id']) for error in errors]
        retry_results, retry_errors = process_batch(retry_ids)
        
        # Update results and errors
        results.update(retry_results)
        errors = retry_errors

    # Save errors to a JSON file
    if errors:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"error_log_{timestamp}.json"
        with open(filename, 'w') as f:
            json.dump(errors, f, indent=2)

    # Update the prize table with the new values
    updated_rows = update_prize_table(results)

    # Print final summary
    print(f"Processed {len(ids)} items:")
    print(f"  Successful: {len(results)}")
    print(f"  Errors: {len(errors)}")
    if errors:
        print(f"Error details saved to {filename}")
    print(f"Updated {updated_rows} rows in the prize table.")

if __name__ == "__main__":
    main()
