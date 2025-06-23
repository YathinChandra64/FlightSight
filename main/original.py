import streamlit as st
import pandas as pd
from amadeus import Client, ResponseError
from datetime import datetime, date, timedelta
import uuid
import os
import logging
import requests
import json
import geopy.distance
import boto3
import numpy as np
from datetime import timezone
import io

st.set_page_config(page_title="Flight Insights Dashboard", layout="wide")

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
AMAMADEUS_CLIENT_ID = os.getenv("AMADEUS_CLIENT_ID")
AMADEUS_CLIENT_SECRET = os.getenv("AMADEUS_CLIENT_SECRET")
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# Validate required environment variables
required_env_vars = {
    "AMADEUS_CLIENT_ID": AMADEUS_CLIENT_ID,
    "AMADEUS_CLIENT_SECRET": AMADEUS_CLIENT_SECRET,
    "OPENWEATHER_API_KEY": OPENWEATHER_API_KEY,
    "S3_BUCKET": S3_BUCKET,
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_KEY
}

missing_vars = [key for key, value in required_env_vars.items() if not value]
if missing_vars:
    st.error(f"Missing required environment variables: {', '.join(missing_vars)}")
    logger.error(f"Missing environment variables: {', '.join(missing_vars)}")
    st.stop()

# Initialize Amadeus client
amadeus = Client(
    client_id=AMADEUS_CLIENT_ID,
    client_secret=AMADEUS_CLIENT_SECRET
)

def save_data(df, filename):
    if df.empty:
        st.warning(f"No data to save for {filename}")
        return None
    df.columns = [col.replace("-", "_").upper() for col in df.columns]
    df = df.astype(str)
    
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )
        s3_key = f"flight_data/{filename}"
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=csv_buffer.getvalue().encode('utf-8')
        )
        st.success(f"Uploaded to S3: s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        st.error(f"S3 upload failed: {str(e)}")
        logger.error(f"S3 upload error for {filename}: {str(e)}")
    csv_buffer.seek(0)
    st.download_button(
        f"Download {filename}",
        data=csv_buffer.getvalue().encode('utf-8'),
        file_name=filename,
        mime="text/csv"
    )
    
    return df

def get_weather_forecast(lat, lon, api_key, departure_date, days=5):
    base_url = "https://api.openweathermap.org/data/2.5/forecast"
    params = {
        'lat': lat,
        'lon': lon,
        'appid': api_key,
        'units': 'metric'
    }
    try:
        response=requests.get(base_url, params=params)
        response.raise_for_status()
        forecast_data=response.json()
        daily_forecasts = {}
        for forecast in forecast_data.get('list', []):
            dt=datetime.fromtimestamp(forecast['dt'], tz=timezone.utc)
            date_str=dt.strftime("%Y-%m-%d")
            if date_str not in daily_forecasts:
                daily_forecasts[date_str] = []
            daily_forecasts[date_str].append(forecast)

        weather_records = []
        start_date = datetime.strptime(departure_date, "%Y-%m-%d")
        for day_offset in range(days):
            target_date = (start_date + timedelta(days=day_offset)).strftime("%Y-%m-%d")
            forecasts = daily_forecasts.get(target_date, [])
            
            if not forecasts:
                logger.warning(f"No forecast data for {target_date} at lat={lat}, lon={lon}")
                continue

            temps = [str(f['main']['temp']) for f in forecasts]
            wind_speeds = [str(f['wind']['speed']) for f in forecasts]
            wind_gusts = [str(f['wind'].get('gust', '')) for f in forecasts]
            wind_directions = [str(f['wind']['deg']) for f in forecasts]
            humidities = [str(f['main']['humidity']) for f in forecasts]
            cloudiness = [str(f['clouds']['all']) for f in forecasts]
            pressures = [str(f['main']['pressure']) for f in forecasts]
            visibilities = [str(f.get('visibility', '')) for f in forecasts]
            rain = ''
            snow = json.dumps({f['dt']: f.get('snow', {}).get('3h', '') for f in forecasts}) if any(f.get('snow') for f in forecasts) else ''
            weather_desc = max(set([f['weather'][0]['description'] for f in forecasts if f['weather']]), 
                             key=[f['weather'][0]['description'] for f in forecasts if f['weather']].count, 
                             default='') if forecasts else ''

            city = forecast_data.get('city', {})
            tz_offset = city.get('timezone', 0)
            weather_data = {
                "FETCH_TIMESTAMP": datetime.utcnow().isoformat(),
                "VISIBILITY": str(np.mean([float(v) for v in visibilities if v]) if any(v for v in visibilities) else ''),
                "WIND_SPEED": str(np.mean([float(w) for w in wind_speeds if w]) if any(w for w in wind_speeds) else ''),
                "WIND_GUST": str(np.mean([float(g) for g in wind_gusts if g]) if any(g for g in wind_gusts) else ''),
                "WIND_DIRECTION": str(np.mean([float(d) for d in wind_directions if d]) if any(d for d in wind_directions) else ''),
                "RAIN": rain,
                "SNOW": snow,
                "WEATHER_DESCRIPTION": weather_desc,
                "TEMPERATURE": str(np.mean([float(t) for t in temps if t]) if any(t for t in temps) else ''),
                "PRESSURE": str(np.mean([float(p) for p in pressures if p]) if any(p for p in pressures) else ''),
                "HUMIDITY": str(np.mean([float(h) for h in humidities if h]) if any(h for h in humidities) else ''),
                "CLOUDINESS": str(np.mean([float(c) for c in cloudiness if c]) if any(c for c in cloudiness) else ''),
                "SUNRISE": (datetime.fromtimestamp(city.get('sunrise', 0), tz=timezone.utc) + 
                           timedelta(seconds=tz_offset)).isoformat() if city.get('sunrise') else '',
                "SUNSET": (datetime.fromtimestamp(city.get('sunset', 0), tz=timezone.utc) + 
                          timedelta(seconds=tz_offset)).isoformat() if city.get('sunset') else '',
                "EVENT_TIME": (start_date + timedelta(days=day_offset)).isoformat(),
                "DEPARTURE_DATE": target_date
            }
            weather_records.append(weather_data)
        
        return weather_records
    except Exception as e:
        logger.error(f"Weather forecast API error for lat={lat}, lon={lon}: {str(e)}")
        return []

def get_airport_info(iata_code):
    try:
        response = amadeus.reference_data.locations.get(keyword=iata_code, subType="AIRPORT")
        return response.data[0] if response.data else None
    except Exception as e:
        logger.error(f"Airport info error for {iata_code}: {str(e)}")
        return None

def get_airline_name(carrier_code):
    try:
        response = amadeus.reference_data.airlines.get(airlineCodes=carrier_code)
        return response.data[0].get("businessName", carrier_code) if response.data else carrier_code
    except Exception as e:
        logger.error(f"Airline name error for {carrier_code}: {str(e)}")
        return carrier_code

def get_aircraft_name(aircraft_code):
    aircraft_mapping = {
        "320": "Airbus A320",
        "73H": "Boeing 737-800",
        "333": "Airbus A330-300",
        "77W": "Boeing 777-300ER",
        "388": "Airbus A380",
        "739": "Boeing 737-900",
        "321": "Airbus A321",
        "788": "Boeing 787-8 Dreamliner",
        "E75": "Embraer 175",
        "CR9": "Bombardier CRJ-900"
    }
    return aircraft_mapping.get(aircraft_code, f"Unknown Aircraft ({aircraft_code})")

def calculate_distance(origin, destination):
    try:
        return str(geopy.distance.distance(
            (origin['latitude'], origin['longitude']),
            (destination['latitude'], destination['longitude'])
        ).km)
    except Exception as e:
        logger.error(f"Distance calculation error: {str(e)}")
        return ''

@st.cache_data(ttl=3600)
def search_locations(query):
    try:
        response = amadeus.reference_data.locations.get(
            keyword=query,
            subType="CITY,AIRPORT",
            page={'limit': 10}
        )
        return [
            {
                "name": loc.get("name", ""),
                "iata": loc.get("iataCode", ""),
                "city": loc.get("address", {}).get("cityName", ""),
                "country": loc.get("address", {}).get("countryName", ""),
                "latitude": loc.get("geoCode", {}).get("latitude"),
                "longitude": loc.get("geoCode", {}).get("longitude"),
                "type": loc.get("subType", "")
            }
            for loc in response.data
            if loc.get("iataCode")
        ]
    except ResponseError as e:
        st.error(f"Location search error: {str(e)}")
        return []

def get_city_name_from_airport(iata_code):
    airport_info = get_airport_info(iata_code)
    if airport_info and airport_info.get('address', {}).get('cityName'):
        return airport_info['address']['cityName']
    return iata_code

def fill_missing_flights(flights_df, start_date, days=5):
    if flights_df.empty:
        st.warning("No flight data to fill.")
        return flights_df
    
    filled_flights = []
    last_flight_data = None
    
    for day_offset in range(days):
        target_date = (start_date + timedelta(days=day_offset)).strftime("%Y-%m-%d")
        day_flights = flights_df[flights_df['DEPARTURE_DATE'] == target_date]
        
        if not day_flights.empty:
            filled_flights.append(day_flights)
            last_flight_data = day_flights.copy()
        elif last_flight_data is not None:
            reused_flights = last_flight_data.copy()
            reused_flights['TRIP_ID'] = [str(uuid.uuid4()) for _ in range(len(reused_flights))]
            reused_flights['DEPARTURE_DATE'] = target_date
            filled_flights.append(reused_flights)
            logger.info(f"Reused flight data for {target_date}")
        else:
            logger.warning(f"No flight data available to fill for {target_date}")
    
    return pd.concat(filled_flights, ignore_index=True) if filled_flights else flights_df

def fill_missing_weather(weather_df, start_date, iata_codes, days=5):
    if weather_df.empty:
        st.warning("No weather data to fill.")
        return weather_df
    
    filled_weather = []
    last_weather_data = {iata: None for iata in iata_codes}
    
    for iata in iata_codes:
        for day_offset in range(days):
            target_date = (start_date + timedelta(days=day_offset)).strftime("%Y-%m-%d")
            day_weather = weather_df[(weather_df['IATA_CODE'] == iata) & (weather_df['DEPARTURE_DATE'] == target_date)]
            
            if not day_weather.empty:
                filled_weather.append(day_weather)
                last_weather_data[iata] = day_weather.copy()
            elif last_weather_data[iata] is not None:
                reused_weather = last_weather_data[iata].copy()
                reused_weather['LOCATION_ID'] = [str(uuid.uuid4()) for _ in range(len(reused_weather))]
                reused_weather['DEPARTURE_DATE'] = target_date
                reused_weather['EVENT_TIME'] = (start_date + timedelta(days=day_offset)).isoformat()
                filled_weather.append(reused_weather)
                logger.info(f"Reused weather data for {iata} on {target_date}")
            else:
                logger.warning(f"No weather data available to fill for {iata} on {target_date}")
    
    return pd.concat(filled_weather, ignore_index=True) if filled_weather else weather_df

st.title("Flight Insights Dashboard")

st.header("Flight Search")
col1, col2 = st.columns(2)

with col1:
    origin_query = st.text_input("Origin City or Airport", value="Hyderabad")
    origin_options = search_locations(origin_query) if origin_query else []
    origin_selection = st.selectbox(
        "Select Origin",
        [f"{loc['city']} ({loc['iata']}) - {loc['country']}" for loc in origin_options],
        key="origin"
    ) if origin_options else st.selectbox("Select Origin", [])

with col2:
    dest_query = st.text_input("Destination City or Airport", value="Paris")
    dest_options = search_locations(dest_query) if dest_query else []
    dest_selection = st.selectbox(
        "Select Destination",
        [f"{loc['city']} ({loc['iata']}) - {loc['country']}" for loc in dest_options],
        key="dest"
    ) if dest_options else st.selectbox("Select Destination", [])

departure_date = st.date_input("Departure Date", value=date(2025, 6, 13))

if st.button("Get Flight Details"):
    if not origin_options or not dest_options:
        st.error("Please enter valid origin and destination")
        st.stop()
    origin = next((loc for loc in origin_options if f"{loc['city']} ({loc['iata']})" in origin_selection), None)
    destination = next((loc for loc in dest_options if f"{loc['city']} ({loc['iata']})" in dest_selection), None)
    
    if not origin or not destination:
        st.error("Please select valid origin and destination")
        st.stop()
    
    if origin['iata'] == destination['iata']:
        st.error("Origin and destination cannot be the same")
        st.stop()

    with st.spinner("Fetching flight data and weather forecasts for the next 5 days..."):
        try:
            flight_details = []
            weather_data = []
            unique_locations = set()
            airport_info_cache = {}
            last_cabin_info = {}
            last_distance_info = {}
            unique_locations.add((origin['iata'], origin['latitude'], origin['longitude'], "Origin"))
            unique_locations.add((destination['iata'], destination['latitude'], destination['longitude'], "Destination"))

            for day_offset in range(5):
                current_date = departure_date + timedelta(days=day_offset)
                st.write(f"Fetching data for {current_date.strftime('%Y-%m-%d')}...")

                try:
                    response = amadeus.shopping.flight_offers_search.get(
                        originLocationCode=origin['iata'],
                        destinationLocationCode=destination['iata'],
                        departureDate=current_date.strftime("%Y-%m-%d"),
                        adults=1,
                        max=10
                    )
                    flights = response.data
                except ResponseError as e:
                    st.warning(f"No flights found for {current_date.strftime('%Y-%m-%d')}: {str(e)}")
                    logger.error(f"Flight search error for {current_date}: {str(e)}")
                    continue
                
                if not flights:
                    st.warning(f"No flights found for {current_date.strftime('%Y-%m-%d')}")
                    continue

                for idx, offer in enumerate(flights):
                    trip_id = str(uuid.uuid4())
                    last_ticketing_date = offer.get('lastTicketingDate', '')
                    
                    for itin_idx, itinerary in enumerate(offer['itineraries']):
                        segments = itinerary['segments']
                        stops = str(len(segments) - 1)
                        
                        for seg_idx, segment in enumerate(segments):
                            dep_airport = segment['departure']['iataCode']
                            arr_airport = segment['arrival']['iataCode']
                            
                            if dep_airport not in airport_info_cache:
                                airport_info_cache[dep_airport] = get_airport_info(dep_airport)
                            if arr_airport not in airport_info_cache:
                                airport_info_cache[arr_airport] = get_airport_info(arr_airport)
                            
                            dep_info = airport_info_cache[dep_airport] or {}
                            arr_info = airport_info_cache[arr_airport] or {}

                            distance = ''
                            route_key = f"{dep_airport}-{arr_airport}"
                            if dep_info and arr_info:
                                dep_geo = dep_info.get('geoCode', {})
                                arr_geo = arr_info.get('geoCode', {})
                                if dep_geo.get('latitude') and dep_geo.get('longitude') and arr_geo.get('latitude') and arr_geo.get('longitude'):
                                    distance = calculate_distance(
                                        {'latitude': dep_geo['latitude'], 'longitude': dep_geo['longitude']},
                                        {'latitude': arr_geo['latitude'], 'longitude': arr_geo['longitude']}
                                    )
                                    last_distance_info[route_key] = distance
                            if not distance and route_key in last_distance_info:
                                distance = last_distance_info[route_key]

                            cabin_key = f"{segment['carrierCode']}-{current_date.strftime('%Y-%m-%d')}"
                            fare_basis = ''
                            fare_conditions = ''
                            cabin = segment.get('co2Emissions', [{}])[0].get('cabin', '') or segment.get('cabin', '')
                            booking_class = segment.get('class', '')
                            
                            traveler_pricings = offer.get('travelerPricings', [])
                            if traveler_pricings:
                                for tp in traveler_pricings:
                                    for fds in tp.get('fareDetailsBySegment', []):
                                        if fds.get('segmentId') == segment['id']:
                                            fare_basis = fds.get('fareBasis', '')
                                            fare_conditions = fds.get('cabin', cabin)
                                            cabin = fds.get('cabin', cabin)
                                            booking_class = fds.get('class', booking_class)
                                            break
                            
                            if not cabin and cabin_key in last_cabin_info:
                                cabin = last_cabin_info[cabin_key].get('cabin', '')
                                booking_class = last_cabin_info[cabin_key].get('booking_class', '')
                                fare_conditions = last_cabin_info[cabin_key].get('fare_conditions', '')
                            
                            if cabin:
                                last_cabin_info[cabin_key] = {
                                    'cabin': cabin,
                                    'booking_class': booking_class,
                                    'fare_conditions': fare_conditions
                                }
                            
                            flight_details.append({
                                "TRIP_ID": trip_id,
                                "FLIGHT_TYPE": "One-way",
                                "FLIGHT_NO": f"{segment['carrierCode']}{segment['number']}",
                                "CARRIER": segment['carrierCode'],
                                "OPERATING_AIRLINE": segment.get('operating', {}).get('carrierCode', segment['carrierCode']),
                                "OPERATING_AIRLINE_NAME": get_airline_name(
                                    segment.get('operating', {}).get('carrierCode', segment['carrierCode'])
                                ),
                                "ORIGIN": dep_airport,
                                "DESTINATION": arr_airport,
                                "ORIGIN_CITY_NAME": dep_info.get('address', {}).get('cityName', get_city_name_from_airport(dep_airport)),
                                "DESTINATION_CITY_NAME": arr_info.get('address', {}).get('cityName', get_city_name_from_airport(arr_airport)),
                                "AIRPORT_NAME_ORIGIN": dep_info.get('name', dep_airport),
                                "AIRPORT_NAME_DESTINATION": arr_info.get('name', arr_airport),
                                "DEPARTURE": segment['departure']['at'],
                                "ARRIVAL": segment['arrival']['at'],
                                "DURATION": segment['duration'],
                                "STOPS": stops,
                                "AIRCRAFT_CODE": segment['aircraft']['code'],
                                "AIRCRAFT_NAME": get_aircraft_name(segment['aircraft']['code']),
                                "CABIN": cabin,
                                "BOOKING_CLASS": booking_class,
                                "FARE_CONDITIONS": fare_conditions,
                                "CHECKED_BAGS": str(offer.get('numberOfBookableSeats', '')),
                                "BASE_PRICE": offer['price']['base'],
                                "TOTAL_PRICE": offer['price']['grandTotal'],
                                "FLIGHT_DISTANCE_KM": distance,
                                "LAST_TICKETING_DATE": last_ticketing_date,
                                "SEGMENT_CABIN_TYPE": cabin,
                                "SOURCE": "Amadeus API",
                                "FARE_BASIS": fare_basis,
                                "DEPARTURE_DATE": current_date.strftime("%Y-%m-%d")
                            })
                            
                            unique_locations.add((dep_airport,
                                                dep_info.get('geoCode', {}).get('latitude', None),
                                                dep_info.get('geoCode', {}).get('longitude', None),
                                                "Stopover"))
                            unique_locations.add((arr_airport,
                                                arr_info.get('geoCode', {}).get('latitude', None),
                                                arr_info.get('geoCode', {}).get('longitude', None),
                                                "Stopover"))

            flight_columns = [
                "TRIP_ID", "FLIGHT_TYPE", "FLIGHT_NO", "CARRIER", "OPERATING_AIRLINE", "OPERATING_AIRLINE_NAME",
                "ORIGIN", "DESTINATION", "ORIGIN_CITY_NAME", "DESTINATION_CITY_NAME", "AIRPORT_NAME_ORIGIN",
                "AIRPORT_NAME_DESTINATION", "DEPARTURE", "ARRIVAL", "DURATION", "STOPS", "AIRCRAFT_CODE",
                "AIRCRAFT_NAME", "CABIN", "BOOKING_CLASS", "FARE_CONDITIONS", "CHECKED_BAGS", "BASE_PRICE",
                "TOTAL_PRICE", "FLIGHT_DISTANCE_KM", "LAST_TICKETING_DATE", "SEGMENT_CABIN_TYPE", "SOURCE", "FARE_BASIS",
                "DEPARTURE_DATE"
            ]
            
            weather_columns = [
                "LOCATION_ID", "IATA_CODE", "LOCATION_TYPE", "LATITUDE", "LONGITUDE", "FETCH_TIMESTAMP",
                "VISIBILITY", "WIND_SPEED", "WIND_GUST", "WIND_DIRECTION", "RAIN", "SNOW", "WEATHER_DESCRIPTION",
                "TEMPERATURE", "PRESSURE", "HUMIDITY", "CLOUDINESS", "SUNRISE", "SUNSET", "EVENT_TIME", "DEPARTURE_DATE"
            ]
            
            flights_df = pd.DataFrame(flight_details)

            for col in flight_columns:
                if col not in flights_df.columns:
                    flights_df[col] = ''

            flights_df = fill_missing_flights(flights_df, departure_date)
            
            iata_codes = set([loc[0] for loc in unique_locations])
            for iata, lat, lon, loc_type in unique_locations:
                if lat is None or lon is None:
                    airport = get_airport_info(iata)
                    if airport:
                        lat = airport['geoCode']['latitude']
                        lon = airport['geoCode']['longitude']
                
                if lat and lon:
                    weather_records = get_weather_forecast(lat, lon, OPENWEATHER_API_KEY, departure_date.strftime("%Y-%m-%d"), days=5)
                    for record in weather_records:
                        record["LOCATION_ID"] = str(uuid.uuid4())
                        record["IATA_CODE"] = iata
                        record["LOCATION_TYPE"] = "Origin" if iata == origin['iata'] else "Destination" if iata == destination['iata'] else "Stopover"
                        record["LATITUDE"] = str(lat)
                        record["LONGITUDE"] = str(lon)
                        weather_data.append(record)
            
            weather_df = pd.DataFrame(weather_data)
            
            for col in weather_columns:
                if col not in weather_df.columns:
                    weather_df[col] = ''
                    
            weather_df = fill_missing_weather(weather_df, departure_date, iata_codes)
            
            flights_df = flights_df[flight_columns]
            weather_df = weather_df[weather_columns]
            
            if not flights_df.empty:
                save_data(flights_df, "flights.csv")
            if not weather_df.empty:
                save_data(weather_df, "weather.csv")
                
            st.subheader("Flight Details (Next 5 Days)")
            st.dataframe(flights_df)
            
            st.subheader("Weather Forecasts (Next 5 Days)")
            st.dataframe(weather_df)
            
            try:
                distance = geopy.distance.distance(
                    (origin['latitude'], origin['longitude']),
                    (destination['latitude'], destination['longitude'])
                ).km
                st.metric("Route Distance", f"{distance:.2f} km")
            except Exception as e:
                logger.warning(f"Route distance calculation failed: {str(e)}")
            
        except ResponseError as e:
            st.error(f"Amadeus API Error: {str(e)}")
        except Exception as e:
            st.error(f"Unexpected error: {str(e)}")
            logger.error(f"Unexpected error: {str(e)}")