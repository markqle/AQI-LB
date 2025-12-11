import json
import os
from pathlib import Path

import requests

API_KEY = os.getenv("AIR_QUALITY_API_KEY", "AIzaSyDI4XIW1g_rOoWL8CJznG8jCAE8YGFT3mM")
BASE_URL = "https://airquality.googleapis.com/v1/currentConditions:lookup"

def get_current_conditions(lat: float, lon: float, language: str = "en") -> dict:
    payload = {
        "universalAqi": True,
        "location": {"latitude": lat, "longitude": lon},
        "extraComputations": [
            "HEALTH_RECOMMENDATIONS",
            "DOMINANT_POLLUTANT_CONCENTRATION",
            "POLLUTANT_CONCENTRATION",
            "LOCAL_AQI",
            "POLLUTANT_ADDITIONAL_INFO",
        ],
        "languageCode": language,
    }
    resp = requests.post(
        f"{BASE_URL}?key={API_KEY}", json=payload, timeout=15
    )
    resp.raise_for_status()
    return resp.json()

if __name__ == "__main__":
    # Example: Long Beach, CA
    data = get_current_conditions(33.864571, -118.168059)
    print(data)

    # Save response to file in the project directory
    output_path = Path(__file__).resolve().parent / "result.json"
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved response to {output_path}")
