import requests
import json
from config import OPENWEATHER_API_KEY,location

cities = location()
def extract():
    if not OPENWEATHER_API_KEY:
        raise ValueError(
            "Missing OPENWEATHER_API_KEY environment variable."
        )

    for city in cities:
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {
            "q": city,
            "appid": OPENWEATHER_API_KEY,
            "units": "metric",
        }
        response = requests.get(url, params=params, timeout=20)
        data = response.json()
        print(f"Sent data for {city}")
