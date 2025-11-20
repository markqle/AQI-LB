import requests
import json
import base64
import datetime
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery


# Define variables for Cloud Functions
bucket_name = 'aqi_lb_warehouse'
project_name = 'aqilbwarehouse'
dataset_name = '5910cherryave'
table_name = 'example_data'

def get_current_air_quality(api_key, latitude, longitude):
    """
    Looks up the current air quality conditions for a given location.

    Args:
        api_key: Your Google Maps Platform API key.
        latitude: The latitude of the location.
        longitude: The longitude of the location.
    """
    
    # The URL for the API endpoint
    url = f"https://airquality.googleapis.com/v1/currentConditions:lookup?key={api_key}"

    # The data payload to send, as specified in your curl command
    payload = {
        "universalAqi": True,
        "location": {
            "latitude": latitude,
            "longitude": longitude
        },
        "extraComputations": [
            "HEALTH_RECOMMENDATIONS",
            "DOMINANT_POLLUTANT_CONCENTRATION",
            "POLLUTANT_CONCENTRATION",
            "LOCAL_AQI",
            "POLLUTANT_ADDITIONAL_INFO"
        ],
        "languageCode": "en"
    }

    # Set the content type header
    headers = {
        'Content-Type': 'application/json'
    }

    try:
        # Make the POST request
        # We use json=payload to automatically serialize the dict to JSON
        # and set the Content-Type header.
        response = requests.post(url, json=payload)

        # Raise an exception if the request was unsuccessful (e.g., 4xx or 5xx)
        response.raise_for_status()

        # Get the JSON data
        data = response.json()
        
        # Write JSON Lines format so each record is one line (even if only one record)
        with open('result.jsonl', 'w') as f:
            f.write(json.dumps(data) + "\n")

        print("Successfully saved data to result.jsonl")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response content: {response.text}")
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred: {req_err}")
    except Exception as err:
        print(f"An unexpected error occurred: {err}")

# --- Example Usage ---

# IMPORTANT: Replace 'YOUR_API_KEY' with your actual Google API key
API_KEY = "AIzaSyDI4XIW1g_rOoWL8CJznG8jCAE8YGFT3mM" 

# Location from your example
LAT = 33.864571
LNG = -118.168059

if API_KEY == "YOUR_API_KEY":
    print("="*50)
    print("WARNING: Please replace 'YOUR_API_KEY' \n         with your actual API key to run this script.")
    print("="*50)
else:
    get_current_air_quality(API_KEY, LAT, LNG)
