import requests
import json
import os
import datetime
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import secretmanager

# --- CONFIGURATION VARIABLES ---
# Adjust these to match your exact resource names
BUCKET_NAME = 'aqi_lb_warehouse'
PROJECT_NAME = 'aqilbwarehouse'
DATASET_NAME = '5910cherryave'
TABLE_NAME = 'aqi_exact_location'
SECRET_ID = 'google_aqi_key' 

# Location to query (Long Beach Example)
LATITUDE = 33.864571
LONGITUDE = -118.168059

# --- UTILITY FUNCTIONS ---

def get_secret_value(secret_id, version_id="latest"):
    """Fetches the secret string from Secret Manager."""
    # Cloud Functions automatically sets GCP_PROJECT or GOOGLE_CLOUD_PROJECT
    project_id = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    
    if not project_id:
        # Fallback for local testing if env var isn't set
        project_id = PROJECT_NAME
    
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    try:
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        print(f"Error accessing secret '{secret_id}' (Name: {name}): {e}")
        raise RuntimeError(f"Failed to retrieve API key. Ensure Service Account has 'Secret Manager Secret Accessor' role. Error: {e}")
    
def get_air_quality_data(api_key, latitude, longitude):
    """
    Looks up the current air quality conditions.
    """
    url = f"https://airquality.googleapis.com/v1/currentConditions:lookup?key={api_key}"

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

    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        # Log the AQI for quick debugging in Cloud Logs
        aqi = data.get('indexes', [{}])[0].get('aqi', 'N/A')
        print(f"Successfully fetched data. AQI: {aqi}")
        return data

    except requests.exceptions.RequestException as e:
        print(f"API Request Error: {e}")
        raise
        
def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    uri = f"gs://{bucket_name}/{destination_blob_name}"
    print(f"File uploaded to GCS: {uri}")
    return uri

def load_data_to_bigquery(gcs_uri, project, dataset, table):
    """Loads a JSONL file from GCS into BigQuery."""
    bigquery_client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND, 
        # autodetect=True # Uncomment if you want BQ to guess the schema
    )

    load_job = bigquery_client.load_table_from_uri(
        gcs_uri, table_id, job_config=job_config
    )

    print(f"Starting BigQuery load job {load_job.job_id}...")
    load_job.result()  # Wait for the job to complete
    print(f"BigQuery load job finished. Loaded {load_job.output_rows} rows into {table_id}.")

# --- CLOUD FUNCTION ENTRY POINT ---

def aqi_to_bigquery(event, context):
    """
    Main entry point triggered by Pub/Sub.
    """
    print("--- Starting Hourly AQI Pipeline ---")
    
    try:
        # 1. FETCH API KEY
        api_key = get_secret_value(SECRET_ID)
        
        # 2. EXTRACT DATA
        raw_data = get_air_quality_data(api_key, LATITUDE, LONGITUDE)
        
        # 3. PREPARE FILE FOR LOAD
        # /tmp is the only writable directory in Cloud Functions
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        local_file_name = f"/tmp/aqi_data_{timestamp}.jsonl"
        gcs_blob_name = f"raw_data/aqi_data_{timestamp}.jsonl"
        
        # Write as Newline Delimited JSON (JSONL)
        with open(local_file_name, 'w') as f:
            f.write(json.dumps(raw_data) + "\n")
        print(f"Data saved locally to {local_file_name}")

        # 4. UPLOAD TO CLOUD STORAGE
        gcs_uri = upload_blob(BUCKET_NAME, local_file_name, gcs_blob_name)
        
        # 5. LOAD TO BIGQUERY
        load_data_to_bigquery(gcs_uri, PROJECT_NAME, DATASET_NAME, TABLE_NAME)

        print("--- Pipeline Complete ---")
        return "Success"

    except Exception as e:
        print(f"Pipeline Failed: {e}")
        # Re-raising the exception causes the Cloud Function to report a failure (CRASH)
        # This is good for alerting/monitoring.
        raise

if __name__ == '__main__':
    # --- Local Testing Block ---
    # Run this locally to test before deploying.
    # Ensure you have run: gcloud auth application-default login
    print("--- Running Local Test ---")
    # Mock the context arguments
    aqi_to_bigquery({}, None)