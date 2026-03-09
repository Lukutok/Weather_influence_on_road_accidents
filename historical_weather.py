import os
import pandas as pd
import requests
import time
import datetime 
import json
from supabase import create_client 

def get_open_meteo_data(city, lat, lon, start_date, end_date):

    '''
    Получает архивные почасовые данные о погоде с Open-Meteo API.
    Поиск погоды происходит на основе частоты и долготы рассматриваемого города.
    '''
    
    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {'latitude': lat,
              'longitude': lon,
              'start_date': start_date, 
              'end_date': end_date,
              'timezone': 'auto',
              'wind_speed_unit': 'ms',
              'hourly': [
                  'temperature_2m',
                  'weather_code',
                  'relative_humidity_2m',
                  'apparent_temperature',
                  'pressure_msl', 
                  'wind_speed_10m',
                  'dew_point_2m', 
                  'precipitation', 
                  'rain',
                  'snowfall', 
                  'cloud_cover'
              ]}

    try:
        response = requests.get(url, params=params)
        data = response.json()
        data = data['hourly'] 
        data['city'] = city
        return pd.DataFrame(data)
        
    except Exception as ex:
        print(f"Ошибка для {city}: {ex}")
        return None 


def get_historical_weather_by_city(city, lat, lon):
    '''
    Собирает полный архив погоды для указанного города с 2015 года по сегодняшний день.

    Функция разбивает временной период на месячные интервалы.
    Результаты каждого запроса объединяются в датафрейм.
    '''
    
    weather_df = pd.DataFrame()
    next_year = datetime.date.today().year + 1
    
    for year in range(2015, next_year):
        
        date_grid = pd.date_range(f'{year}-01-01', f'{year + 1}-01-01', freq='MS')
    
        for month in range(12):
        
            start_date = date_grid[month].date()
            end_date = date_grid[month + 1].date() - datetime.timedelta(days=1)
        
            if end_date >= datetime.date.today():
                end_date = datetime.date.today()
                
                new_weather_df = get_open_meteo_data(city, lat, lon, start_date, end_date)
                weather_df = pd.concat([weather_df, new_weather_df], ignore_index=True)
                print(start_date, '->', end_date)
                break
                
            else:
                new_weather_df = get_open_meteo_data(city, lat, lon, start_date, end_date)
                weather_df = pd.concat([weather_df, new_weather_df], ignore_index=True)
                
            print(start_date, '->', end_date)
            time.sleep(1)

    return weather_df


def get_weather(code):
    '''
    Преобразует числовой код погоды в текстовое описание.
    '''

    if code == 0:
        return 'Ясно'
        
    elif code in [1, 2, 3]:
        return 'Облачно'
        
    elif code in [45, 48]:
        return 'Туман'
        
    elif code in [51, 53, 55]:
        return 'Морось'

    elif code in [56, 57]:
        return 'Замерзающая морось'

    elif code in [61, 63, 65]:
        return 'Дождь'

    elif code in [66, 67]:
        return 'Ледяной дождь'

    elif code in [71, 73, 75]:
        return 'Снег'

    elif code in [77]:
        return 'Снежные зерна'

    elif code in [80, 81, 82]:
        return 'Ливень'

    elif code in [85, 86]:
        return 'Пурга'

    elif code == 95:
        return 'Гроза'

    elif code in [96, 99]:
        return 'Гроза с градом' 

    else:
        return 'Не найдено'


def preprocess_weather_data(weather_df):

    if weather_df is None:
        return None
        
    else:
        weather_df['weather_code'] = weather_df['weather_code'].astype('int')
        weather_df['weather_code'] = weather_df['weather_code'].apply(get_weather)
        weather_df = weather_df.rename(columns={'time': 'datetime'})
        weather_df['datetime'] = pd.to_datetime(weather_df['datetime'])
        return weather_df
        

def get_supabase_client():
    '''
    Получение клиента 
    '''
    
    project_url = 'https://lpdaqqydnpvynwymzxxt.supabase.co'
    api_key = os.getenv('API_KEY')
    
    supabase = create_client(project_url, api_key)
    return supabase


def check_data_in_database(city, date):
    """
    Проверяеv, есть ли данные в базе данных для города за определенную дату
    """
    supabase = get_supabase_client()
    
    try:
        response = (supabase.table('weather')
                            .select('city', count='exact', head=True)
                            .eq('city', city)
                            .eq('datetime', date)
                            .execute())
        
        return response.count > 0
        
    except Exception as e:
        print(f"Ошибка при проверке данных в БД: {e}")
        return False


def load_weather_data_to_database(df):

    if df is None:
        print('Нет данных для загрузки')
        return

    else:
        supabase = get_supabase_client()
    
        json_string = df.to_json(orient='records', date_format='iso')
        data_to_insert = json.loads(json_string)
    
        try:
            response = supabase.table('weather').insert(data_to_insert).execute()
            print("Успешно загружено!")
            
        except Exception as e:
            print(f"Ошибка: {e}")


weather_df_ekb = get_historical_weather_by_city('Екатеринбург', 56.8389, 60.6057)
weather_df_perm = get_historical_weather_by_city('Пермь', 58.0109, 56.2319)

weather_df_ekb = preprocess_weather_data(weather_df_ekb)
weather_df_perm = preprocess_weather_data(weather_df_perm)

load_weather_data_to_database(weather_df_ekb)
load_weather_data_to_database(weather_df_perm)

