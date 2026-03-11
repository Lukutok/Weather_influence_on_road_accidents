import pandas as pd
import datetime 
import json
from historical_weather import check_data_in_database, get_open_meteo_data, preprocess_weather_data, load_weather_data_to_database

if __name__ == "__main__":
    
    yesterday = datetime.date.today() - datetime.timedelta(days=1)

    cities = [{'name': 'Екатеринбург', 'lat': 56.8389, 'lon': 60.6057}, 
              {'name': 'Пермь', 'lat': 58.0109, 'lon': 56.2319}]
    
    weather_data = []

    for city in cities:
        
        if check_data_in_database(city['name'], yesterday):
            print(f"Данные за {yesterday} для города {city['name']} уже в базе")
            
        else:            
            weather_by_city = get_open_meteo_data(city['name'], city['lat'], city['lon'], yesterday, yesterday)
            weather_by_city = preprocess_weather_data(weather_by_city)
            
            if weather_by_city is not None:
                weather_data.append(weather_by_city)

    if weather_data:
        weather_data = pd.concat(weather_data, ignore_index=True)
        load_weather_data_to_database(weather_data)
    else:
        print('Данных для выгрузки нет')
