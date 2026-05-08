from datetime import datetime,timezone

def transform_data(data):
    return {
        "city": data["name"],
        "country": data["sys"]["country"],
        "timestamp": datetime.fromtimestamp(data["dt"], tz=timezone.utc),
        "timestamp": datetime.now(timezone.utc),   
        "feels_like_celsius": round(data["main"]["feels_like"], 2),
        "temp_min_celsius": round(data["main"]["temp_min"], 2),
        "temp_max_celsius": round(data["main"]["temp_max"], 2),
        "humidity": data["main"]["humidity"],
        "pressure_hpa": data["main"]["pressure"],
        "wind_speed_ms": data["wind"]["speed"],
        "wind_deg": data["wind"].get("deg"),
        "weather_main": data["weather"][0]["main"],
        "weather_description": data["weather"][0]["description"],
        "clouds_percent": data["clouds"]["all"],
        "visibility_meters": data.get("visibility"),
        "sunrise": datetime.fromtimestamp(data["sys"]["sunrise"], tz=timezone.utc),
        "sunset": datetime.fromtimestamp(data["sys"]["sunset"], tz=timezone.utc)  # ✓ fixed
    }
    