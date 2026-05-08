import requests
from pipeline.config import  location
from airflow.models import Variable

cities = location()

def extract():
    OPENWEATHER_API_KEY =  Variable.get("openweather_api_key")
    if not OPENWEATHER_API_KEY:
        raise ValueError("Missing OPENWEATHER_API_KEY")

    cities = location()
    results = []
    failed = 0

    for city in cities:
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {
            "q": city,
            "appid": OPENWEATHER_API_KEY,
            "units": "metric",
        }

        try:
            response = requests.get(url, params=params, timeout=20)

            if response.status_code != 200:
                print(f"❌ Failed for {city}: {response.text}")
                failed += 1
                continue

            results.append(response.json())
            print(f"✅ Success: {city}")

        except requests.exceptions.RequestException as e:
            print(f"⚠️ Request error for {city}: {e}")
            failed += 1

    # 🔥 IMPORTANT: prevent silent broken pipeline
    if len(results) == 0:
        raise Exception("All API requests failed")

    print(f"Finished: {len(results)} success, {failed} failed")

    return results