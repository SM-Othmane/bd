import requests
import time
from datetime import datetime, timedelta
import schedule
from ratelimit import limits, sleep_and_retry
import os
import sys
import dotenv
import json
import logging
import re

dotenv.load_dotenv()

OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
TOMTOM_API_KEY = os.getenv('TOMTOM_API_KEY')

OPENWEATHER_CALLS = 60
OPENWEATHER_PERIOD = 60
TOMTOM_DAILY_LIMIT = 2500
TOMTOM_CALLS_BUFFER = 100
TOMTOM_CALLS = (TOMTOM_DAILY_LIMIT - TOMTOM_CALLS_BUFFER) // 24

HOSPITALS = {
    'PLATEFORME_LOGISTIQUE': {
        'name': "Plateforme Logistique AP-HM",
        'lat': "43.3502",
        'lon': "5.3615"
    },
    'TIMONE': {
        'name': "Hôpital de la Timone",
        'lat': "43.2899",
        'lon': "5.4033"
    },
    'NORD': {
        'name': "Hôpital Nord",
        'lat': "43.3805", 
        'lon': "5.4027"
    },
    'CONCEPTION': {
        'name': "Hôpital de la Conception",
        'lat': "43.2889",
        'lon': "5.3947"
    },
    'SAINTE_MARGUERITE': {
        'name': "Hôpital Sainte-Marguerite",
        'lat': "43.2657",
        'lon': "5.4027"
    }
}

MARSEILLE = {
    'lat': "43.2965",
    'lon': "5.3698"
}

@sleep_and_retry
@limits(calls=OPENWEATHER_CALLS, period=OPENWEATHER_PERIOD)
def get_weather_data(lat, lon):
    base_url = "https://api.openweathermap.org/data/2.5"
    
    try:
        weather_url = f"{base_url}/weather?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric"
        weather_response = requests.get(weather_url)
        weather_response.raise_for_status()
        weather_response = weather_response.json()
        
        air_url = f"{base_url}/air_pollution?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}"
        air_response = requests.get(air_url)
        air_response.raise_for_status()
        air_response = air_response.json()
        
        forecast_url = f"{base_url}/forecast?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric"
        forecast_response = requests.get(forecast_url)
        forecast_response.raise_for_status()
        forecast_response = forecast_response.json()
        
        weather_info = {
            'timestamp': datetime.now().isoformat(),
            'current': {
                'temperature': weather_response['main']['temp'],
                'feels_like': weather_response['main']['feels_like'],
                'humidity': weather_response['main']['humidity'],
                'pressure': weather_response['main']['pressure'],
                'wind_speed': weather_response['wind']['speed'],
                'wind_direction': weather_response['wind'].get('deg'),
                'description': weather_response['weather'][0]['description'],
                'rain_1h': weather_response.get('rain', {}).get('1h', 0),
                'visibility': weather_response.get('visibility')
            },
            'air_quality': {
                'aqi': air_response['list'][0]['main']['aqi'],
                'components': air_response['list'][0]['components']
            },
            'forecast_3h': [
                {
                    'timestamp': item['dt_txt'],
                    'temperature': item['main']['temp'],
                    'precipitation_prob': item.get('pop', 0),
                    'rain_3h': item.get('rain', {}).get('3h', 0)
                }
                for item in forecast_response['list'][:3]
            ]
        }
        print(f"Weather data collected at {datetime.now()}")
        return weather_info
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None

@sleep_and_retry
@limits(calls=TOMTOM_CALLS, period=3600)
def get_traffic_data(origin_lat, origin_lon, dest_lat, dest_lon):
    try:
        current_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S+01:00')
        
        route_url = f"https://api.tomtom.com/routing/1/calculateRoute/{origin_lat},{origin_lon}:{dest_lat},{dest_lon}/json"
        
        route_params = {
            'key': TOMTOM_API_KEY,
            'traffic': 'true',
            'routeType': 'fastest',
            'travelMode': 'truck',
            'vehicleMaxSpeed': '90',
            'instructionsType': 'tagged',
            'routeRepresentation': 'polyline',
            'computeTravelTimeFor': 'all',
            'departAt': current_time,
            'sectionType': 'traffic',
            'language': 'fr-FR'
        }
        
        print(f"Requesting route with params: {route_params}")
        
        route_response = requests.get(route_url, params=route_params)
        route_response.raise_for_status()
        route_data = route_response.json()
        
        if 'routes' not in route_data:
            print(f"No routes found in response: {route_data}")
            return None
        
        traffic_info = {
            'timestamp': datetime.now().isoformat(),
            'main_route': {
                'distance_meters': route_data['routes'][0]['summary']['lengthInMeters'],
                'time_seconds': route_data['routes'][0]['summary']['travelTimeInSeconds'],
                'traffic_delay_seconds': route_data['routes'][0]['summary'].get('trafficDelayInSeconds', 0),
                'traffic_confidence': route_data['routes'][0].get('confidence', 0),
                'instructions': []
            }
        }
        
        # Process guidance instructions if available
        if 'guidance' in route_data['routes'][0]:
            for instruction in route_data['routes'][0]['guidance']['instructions']:
                # Clean up the message by removing allXML tags
                message = instruction.get('message', '')
                # Remove all XML tags using regex
                message = re.sub(r'<[^>]+>', '', message)
                
                instruction_info = {
                    'message': message,
                    'distance_meters': instruction.get('routeOffsetInMeters', 0),
                    'street': instruction.get('street', ''),
                    'exit_number': instruction.get('exitNumber', ''),
                    'turn_angle': instruction.get('turnAngleInDecimalDegrees', 0),
                    'latitude': instruction.get('point', {}).get('latitude'),
                    'longitude': instruction.get('point', {}).get('longitude'),
                    'type': instruction.get('type', ''),
                    'road_numbers': instruction.get('roadNumbers', []),
                    'maneuver': instruction.get('maneuver', '')
                }
                traffic_info['main_route']['instructions'].append(instruction_info)
        
        # Add route legs with points
        if 'legs' in route_data['routes'][0]:
            traffic_info['main_route']['route_legs'] = [
                {
                    'distance': leg['summary']['lengthInMeters'],
                    'travel_time': leg['summary']['travelTimeInSeconds'],
                    'delay': leg.get('trafficDelayInSeconds', 0),
                    'start_point': {
                        'lat': leg['points'][0]['latitude'],
                        'lon': leg['points'][0]['longitude']
                    },
                    'end_point': {
                        'lat': leg['points'][-1]['latitude'],
                        'lon': leg['points'][-1]['longitude']
                    },
                    'guidance': leg.get('guidance', []),
                    'points': [
                        {
                            'lat': point['latitude'],
                            'lon': point['longitude']
                        }
                        for point in leg['points']
                    ]
                }
                for leg in route_data['routes'][0]['legs']
            ]
        
        print("Route data successfully processed")
        return traffic_info
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching traffic data: {e}")
        print(f"Response content: {e.response.content if hasattr(e, 'response') else 'No response content'}")
        return None
    except KeyError as e:
        print(f"Error parsing response data: {e}")
        print(f"Route data structure: {json.dumps(route_data, indent=2)}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        print(f"Full error details: {str(e)}")
        return None

def setup_schedule():
    schedule_info = []
    
    for hour in [6, 7, 8, 9, 16, 17, 18, 19]:
        for minute in [0, 15, 30, 45]:
            time_str = f"{hour:02d}:{minute:02d}"
            schedule.every().day.at(time_str).do(collect_data)
            schedule_info.append(f"- {time_str}")
    
    for hour in [10, 11, 12, 13, 14, 15, 20, 21, 22]:
        for minute in [0, 30]:
            time_str = f"{hour:02d}:{minute:02d}"
            schedule.every().day.at(time_str).do(collect_data)
            schedule_info.append(f"- {time_str}")
    
    for hour in [23, 0, 1, 2, 3, 4, 5]:
        time_str = f"{hour:02d}:00"
        schedule.every().day.at(time_str).do(collect_data)
        schedule_info.append(f"- {time_str}")
    
    logging.info("Scheduled collection times:")
    for time in sorted(schedule_info):
        logging.info(time)
    
    return schedule_info

def start_scheduled_collection():
    setup_schedule()
    
    logging.info("Performing initial collection...")
    collect_data()
    
    logging.info("Starting schedule loop. Press Ctrl+C to stop.")
    last_run = datetime.now()
    
    while True:
        schedule.run_pending()
        
        current_time = datetime.now()
        if (current_time - last_run).total_seconds() >= 3600:
            logging.info("Schedule is running... (hourly heartbeat)")
            last_run = current_time
        
        time.sleep(60)

def setup_logging():
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'logs/collection_{datetime.now().strftime("%Y%m%d")}.log'),
            logging.StreamHandler()
        ]
    )

def save_to_json(data, data_type, location):
    data_dir = 'collected_data'
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{data_dir}/{data_type}_{location}_{timestamp}.json"
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logging.info(f"Data saved to {filename}")

def collect_data():
    collection_start = datetime.now()
    logging.info("Starting data collection...")
    
    try:
        for hospital_name, hospital in HOSPITALS.items():
            logging.info(f"Collecting weather data for {hospital_name}")
            weather_data = get_weather_data(hospital['lat'], hospital['lon'])
            if weather_data:
                logging.info(f"✅ Weather data collected for {hospital_name}")
                save_to_json(weather_data, 'weather', hospital_name)
            else:
                logging.error(f"❌ Failed to collect weather data for {hospital_name}")
        
        origin = HOSPITALS['PLATEFORME_LOGISTIQUE']
        for dest_key, destination in HOSPITALS.items():
            if dest_key != 'PLATEFORME_LOGISTIQUE':
                logging.info(f"Collecting traffic data from Plateforme to {dest_key}")
                traffic_data = get_traffic_data(
                    origin['lat'], origin['lon'],
                    destination['lat'], destination['lon']
                )
                if traffic_data:
                    logging.info(f"✅ Traffic data collected for route to {dest_key}")
                    save_to_json(traffic_data, 'traffic', f"PF_to_{dest_key}")
                else:
                    logging.error(f"❌ Failed to collect traffic data for route to {dest_key}")
        
        collection_end = datetime.now()
        duration = (collection_end - collection_start).total_seconds()
        logging.info(f"Data collection completed in {duration:.2f} seconds")
        
    except Exception as e:
        logging.error(f"Error during data collection: {str(e)}")

def test_single_collection():
    print("\n=== Starting Test Collection ===")
    
    print("\nTesting Weather API for Marseille city center:")
    weather_result = get_weather_data(MARSEILLE['lat'], MARSEILLE['lon'])
    if weather_result:
        print("✅ Weather API test successful")
        print("Weather data:", json.dumps(weather_result, indent=2))
    else:
        print("❌ Weather API test failed")

    print("\nTesting Traffic API:")
    origin = HOSPITALS['PLATEFORME_LOGISTIQUE']
    destination = HOSPITALS['TIMONE']
    print(f"\nTesting traffic data from {origin['name']} to {destination['name']}")
    traffic_result = get_traffic_data(
        origin['lat'], origin['lon'],
        destination['lat'], destination['lon']
    )
    if traffic_result:
        print("✅ Traffic API test successful")
        print("Traffic data:", json.dumps(traffic_result, indent=2))
    else:
        print("❌❌ Traffic API test failed")
    
    print("\n=== Test Collection Complete ===")

def main():
    setup_logging()
    logging.info("=== Weather and Traffic Data Collection Tool Started ===")
    
    print("\nData Collection Tool")
    print("==================")
    print("1. Test mode (single collection)")
    print("2. Regular mode (continuous collection)")
    print("3. Timed collection (2 hours)")
    
    try:
        mode = input("\nEnter mode (1, 2, or 3): ").strip()
        
        if mode == "1":
            logging.info("Starting test mode...")
            test_single_collection()
            
        elif mode == "2":
            logging.info("Starting scheduled collection mode...")
            start_scheduled_collection()
            
        elif mode == "3":
            hours = 2
            print(f"\nStarting {hours}-hour test collection...")
            
            start_time = datetime.now()
            end_time = start_time + timedelta(hours=hours)
            
            print(f"\nCollection Schedule:")
            print("-------------------")
            print("- Peak hours (6-10 AM, 4-8 PM): Every 15 minutes")
            print("- Off-peak hours: Every 30 minutes")
            print("- Night hours: Every hour")
            
            setup_schedule()
            
            print("\nPerforming initial collection...")
            collect_data()
            
            print(f"\nCollection will run until: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("Data is being saved in the 'collected_data' directory")
            print("\nPress Ctrl+C to stop earlier")
            
            while datetime.now() < end_time:
                schedule.run_pending()
                time.sleep(60)
                
            print("\n✅ Collection period completed!")
            print(f"Check 'collected_data' directory for the collected data")
            
        else:
            print("Invalid mode selected. Please run again and select 1, 2, or 3.")
            
    except KeyboardInterrupt:
        print("\n\nScript stopped by user.")
        print("Check 'collected_data' directory for the collected data")
    except Exception as e:
        print(f"\nAn error occurred: {e}")

if __name__ == "__main__":
    if not os.getenv('OPENWEATHER_API_KEY'):
        print("❌ Error: OPENWEATHER_API_KEY not found in environment variables")
        sys.exit(1)
    if not os.getenv('TOMTOM_API_KEY'):
        print("❌ Error: TOMTOM_API_KEY not found in environment variables")
        sys.exit(1)
    
    main()