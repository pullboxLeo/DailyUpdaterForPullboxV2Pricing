import os
from dotenv import load_dotenv
from urllib.parse import urlparse
import psycopg2
import requests
import uuid
import json
import math

def get_color_for_coin_value(coin_value):
    if coin_value >= 100:
        return '#F7CA0F'  # Orange
    if coin_value >= 50:
        return '#B723F2'  # Purple
    if coin_value >= 20:
        return '#18B9FF'  # Blue
    if coin_value >= 5:
        return '#2DC257'  # Green
    return '#6b7280'      # Gray

def query_box_table():
    load_dotenv()
    database_url = os.getenv('PRODUCTION_DATABASE_URL')
    pullbox_api_key = os.getenv('PRODUCTION_PULLBOX_API_KEY')
    pullbox_api_url = os.getenv('PRODUCTION_PULLBOX_API_URL')
    headers = {
        "Authorization": pullbox_api_key,
        "Content-Type": "application/json"
    }
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

        # Get box data
        cur.execute("SELECT id, name, image_url, slug, is_live, category, tags, splash_image, edge, is_hidden from box where is_live = True and LOWER(name) NOT LIKE '%rewards%'")
        rows = cur.fetchall()
        
        for box_row in rows:
            # Get all cards for this box
            cur.execute("select name, weight, value, condition, set, finish, mass, mass_unit, image, withdrawable, id from prize where box_id = %s and is_deleted = False", (box_row[0],))
            card_rows = cur.fetchall()
            
            # Debug prints
            print("\nCalculating box value:")
            
            # Calculate total weighted value (value * 146 * weight)
            total_weighted_value = sum((float(card[2]) * 146 * int(float(card[1]))) if card[2] and card[1] else 0 for card in card_rows)
            print(f"Total weighted value: {total_weighted_value}")
            
            # Calculate total weight
            total_weight = sum(int(float(card[1])) if card[1] else 0 for card in card_rows)
            print(f"Total weight: {total_weight}")
            
            if total_weight > 0:
                edge = float(box_row[8]) if box_row[8] else 12
                print(f"Edge: {edge}")
                
                # Calculate total box value using your formula
                total_box_value = math.floor(
                    round(total_weighted_value) / total_weight / (100 - edge) * 100
                ) / 100
                print(f"Total box value: {total_box_value}")
            else:
                total_box_value = 0
                print("Total box value defaulted to 0 due to zero weight")
                
            box_color = get_color_for_coin_value(float(total_box_value))
            print(f"Calculated color: {box_color}")
            
            # Construct box JSON with its cards
            box_data = {
                "id": str(box_row[0]),
                "name": box_row[1],
                "slug": box_row[3],
                "image": box_row[2],
                "splash_image": box_row[7],
                "categories": [box_row[5]] if box_row[5] else [],
                "tags": box_row[6] if box_row[6] else [],
                "is_live": bool(box_row[4]),
                "edge": int(box_row[8]) if box_row[8] else 12,
                "is_hidden": bool(box_row[9]),
                "color": box_color,
                "items": []
            }
            
            # Add each card to the items array
            for card in card_rows:
                # Convert value: multiply by 100, then by 1.46, then round to integer
                raw_value = float(card[2]) if card[2] else 0
                adjusted_value = round(raw_value * 100 * 1.46)
                
                item = {
                    "external_id": card[10],
                    "name": card[0],
                    "image": card[8],
                    "value": adjusted_value,  # Using the adjusted value
                    "withdrawable": bool(card[9]),
                    "mass": int(float(card[6])) if card[6] else 10,  # Ensure it's a number
                    "mass_unit": card[7] or "g",
                    "weight": int(float(card[1])) if card[1] else 100,  # Ensure it's a number
                    "display_properties": [
                        {
                            "name": "Set",
                            "value": card[4] or "",
                            "detail_level": "BASIC"
                        },
                        {
                            "name": "Condition",
                            "value": card[3] or "",
                            "detail_level": "BASIC"
                        },
                        {
                            "name": "Finish",
                            "value": card[5] or "",
                            "detail_level": "BASIC"
                        }
                    ]
                }
                box_data["items"].append(item)
            
            # Before sending the request, save the JSON data to a file
            with open('debug_last_request.json', 'w') as f:
                json.dump(box_data, f, indent=2)
            
            # Send the request
            try:
                response = requests.post(
                    pullbox_api_url, 
                    headers=headers, 
                    json=box_data,
                    timeout=(25, 45)  # (connect_timeout, read_timeout) in seconds
                )
                print(f"Response Status Code: {response.status_code}")
                print(f"Response Content: {response.text}")
                
                if response.ok:
                    print(f"Request successful for box {box_data['name']}!")
                else:
                    print(f"Request failed with status code {response.status_code}")
                    print(f"Error message: {response.text}")
            except requests.exceptions.RequestException as e:
                print(f"Error sending POST request:")
                print(e)

        ids = [(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9]) for row in rows]
        for id in ids:
            print(id)
            cur.execute("select name, weight, value, condition, set, finish, mass, mass_unit, withdrawable from prize where box_id = %s", (id[0],))
            rows = cur.fetchall()
            # for row in rows:
            #     print(row)
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

if __name__ == "__main__":
    query_box_table()
