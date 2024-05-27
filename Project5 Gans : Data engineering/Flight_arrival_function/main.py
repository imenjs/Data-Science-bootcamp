import functions_framework
import pandas as pd
import sqlalchemy
import requests
from datetime import datetime, timedelta
from pytz import timezone

@functions_framework.http

def insert(request):
    connection_string = connection()
    insert_into_flight_table(connection_string)
    return 'Data successfully added'

def connection():
    connection_name = "lithe-sonar-416410:europe-west1:wbs-mysql-db"
    db_user = "root"
    db_password = "password"
    schema_name = "flight_arrival"

    driver_name = 'mysql+pymysql'
    query_string = {"unix_socket": f"/cloudsql/{connection_name}"}

    db = sqlalchemy.create_engine(
        sqlalchemy.engine.url.URL(
            drivername=driver_name,
            username=db_user,
            password=db_password,
            database=schema_name,
            query=query_string,
        )
    )
    return db

def insert_into_flight_table(con_str):
    icao_list1 = ["EDDF", "EDDB", "EDDM", "EDDK", "EDDL", "EDDH", "EDDP", "EDDV"]
    flight_arrivals = tomorrows_flight_arrivals(icao_list1)

    flight_arrivals['arrival_time'] = pd.to_datetime(flight_arrivals['arrival_time'])
    flight_arrivals['data_retrieved_on'] = pd.to_datetime(flight_arrivals['data_retrieved_on'])
    flight_arrivals = flight_arrivals.rename(columns={"arrival_airport_icao": "icao"})

    flight_arrivals.to_sql(name="flight_arrivals", con=con_str, if_exists='append', index=False)

def tomorrows_flight_arrivals(icao_list):
    api_key = "14a44098c8mshe4536a007985112p1e3b4bjsn8fd805eb6bd4"
    berlin_timezone = timezone('Europe/Berlin')
    today = datetime.now(berlin_timezone).date()
    tomorrow = (today + timedelta(days=1))

    list_for_arrivals_df = []

    for icao in icao_list:
        times = [["00:00","11:59"],["12:00","23:59"]]

        for time in times:
            url = f"https://aerodatabox.p.rapidapi.com/flights/airports/icao/{icao}/{tomorrow}T{time[0]}/{tomorrow}T{time[1]}"
            querystring = {"direction":"Arrival","withCancelled":"false"}

            headers = {
                "X-RapidAPI-Key": "12c7bd34d2msha978e1852a9e04dp17b222jsnd5260f196022",
                "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
            }

            response = requests.request("GET", url, headers=headers, params=querystring)
            flights_resp = response.json()

            if "arrivals" in flights_resp:
                arrivals = flights_resp["arrivals"]
                for arrival in arrivals:
                    flight_number = arrival.get("number")
                    airline = arrival.get("airline", {}).get("name")
                    scheduled_time = arrival.get("movement", {}).get("scheduledTime", {}).get("local")
                    terminal = arrival.get("movement", {}).get("terminal")
                    departure_city = arrival.get("movement", {}).get("airport", {}).get("name")
                    departure_icao = arrival.get("movement", {}).get("airport", {}).get("icao")
                    aircraft = arrival.get("aircraft", {}).get("model")

                    arrival_data = {
                        "arrival_airport_icao": icao,
                        "flight_number": flight_number,
                        "airline": airline,
                        "arrival_time": scheduled_time,
                        "arrival_terminal": terminal,
                        "departure_city": departure_city,
                        "departure_airport_icao": departure_icao,
                        "data_retrieved_on": datetime.now().date(),
                        "aircraft" : aircraft
                    }

                    list_for_arrivals_df.append(arrival_data)
            else:
                print(f"No arrivals found for {icao} at {tomorrow}T{time[0]} - {tomorrow}T{time[1]}")

    return pd.DataFrame(list_for_arrivals_df)
