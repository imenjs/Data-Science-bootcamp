import functions_framework
import pandas as pd
import sqlalchemy
import requests

@functions_framework.http
def insert(request):
    connection_string = connection()
    insert_into_weather_table(connection_string)
    return 'Data successfully added'

def connection():
    connection_name = "lithe-sonar-416410:europe-west1:wbs-mysql-db"
    db_user = "root"
    db_password = "password"
    schema_name = "weather_cloud"

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

def insert_into_weather_table(con_str):
    cities = ['Munich', 'Berlin', 'Cologne', 'Hamburg', 'Frankfurt', 'Leipzig', "Hannover"]
    key = "3d0bdd473ef1150bae09f7c484a8be5c"
    weather_df = get_weather_data(cities, key)
    weather_df['Temperature'] = (weather_df['Temperature'] - 273.15).round(2)
    weather_df['feel_like_temp'] = (weather_df['feel_like_temp'] - 273.15).round(2)
    weather_df['Forcast_Date'] = pd.to_datetime(weather_df['Forcast_Date'])
    weather_df["snow"] = weather_df["snow"].astype(float)
    weather_df.to_sql(name="weather_df", con=con_str, if_exists='append', index=False)

def get_weather_data(cities, api_key):
    weather_data = []

    for city in cities:
        url = f"http://api.openweathermap.org/data/2.5/forecast?q={city}&appid={api_key}"
        response = requests.get(url)
        data = response.json()
        weather_data.append(data)
    
    city_data = []
    
    for data in weather_data:
        city_name = data["city"]["name"]
        repetitions = data["cnt"]
        for i in range(repetitions):
            forecast = data["list"][i]
            temp = forecast["main"]["temp"]
            humidity = forecast["main"]["humidity"]
            weather = forecast["weather"][0]["main"]
            description = forecast["weather"][0]["description"]
            wind_speed = forecast["wind"]["speed"]
            date = forecast["dt_txt"]
            feel_like_temp = forecast["main"]["feels_like"]
            
            # Check if "Rain" is in the weather description
            if (forecast["weather"][0]["main"] == "Rain"):
                rain = forecast["rain"]["3h"]
            else:
                rain = 0     
            if (forecast["weather"][0]["main"] == "Snow"):
                snow = forecast["snow"]["3h"]
            else:
                snow = 0 
            
            city_data.append({
                #"city": city_name,
                "Temperature": temp,
                "feel_like_temp": feel_like_temp,
                "Humidity": humidity,
                "Weather": weather,
                "Description": description,
                "Wind_Speed": wind_speed,
                "rain": rain,
                "snow": snow,
                "Forcast_Date": date
            })
    
    weather_df = pd.DataFrame(city_data)
    return weather_df
