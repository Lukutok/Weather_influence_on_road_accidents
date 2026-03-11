import pandas as pd
import requests
from io import StringIO
import json
from supabase import create_client
import time
from datetime import datetime
import logging
import sys
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

def load_data_to_database(df, table_name):
    
    project_url = 'https://lpdaqqydnpvynwymzxxt.supabase.co'
    api_key = ///
    
    supabase = create_client(project_url, api_key)
    
    json_string = df.to_json(orient='records', date_format='iso')
    data_to_insert = json.loads(json_string)
    
    try:
        response = supabase.table(table_name).insert(data_to_insert).execute()
        print("Успешно загружено!")
        
    except Exception as e:
        print(f"Ошибка: {e}")

# ссылка на википедию с таблицей городов 
url = 'https://ru.wikipedia.org/wiki/%D0%A1%D0%BF%D0%B8%D1%81%D0%BE%D0%BA_%D0%B3%D0%BE%D1%80%D0%BE%D0%B4%D0%BE%D0%B2_%D0%A0%D0%BE%D1%81%D1%81%D0%B8%D0%B8'



headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    tables = pd.read_html(StringIO(response.text))
    
    df = tables[0]
    print('Данные получены')
    
else:
    print("Ошибка доступа к сайту:", response.status_code)


geolocator = Nominatim(user_agent="my_geo_app_123")

geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

def get_coordinates(city_name):
    try:
        # Добавляем ", Россия", чтобы не найти одноименный город в другой стране
        location = geocode(city_name + ", Россия")
        
        if location:
            return location.latitude, location.longitude
        return None, None
    except:
        return None, None


df['coords'] = df['Город'].apply(get_coordinates)
df[['lat', 'lon']] = pd.DataFrame(df['coords'].tolist(), index=df.index)

df = df[['Город', 'Регион', 'Федеральный округ', 'Население', 'lat', 'lon']]
df = df.rename(columns={'Город': 'city', 'Регион': 'region', 'Федеральный округ': 'federal_district', 'Население': 'population'})

df['population'] = df['population'].str.replace(r'\[.*', '', regex=True)
df['population'] = df['population'].str.replace(' ', '')
df['population'] = df['population'].astype('int')
df = df.where(pd.notnull(df), None)
load_data_to_database(df, 'cities')

'''
Получение ОКАТО код с сайта ГИБДД в формате json 
'''

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('gibdd_okato.log', mode='a', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger()

#дата всегда прошлый месяц
def get_all_regions():
    now = datetime.now()
    year = now.year
    month = now.month - 1 if now.month > 1 else 12
    if month == 12:
        year -= 1
    
    logger.info("Получаем список регионов РФ...")  
    rf_payload = {
        "maptype": 1,
        "region": "877", 
        "date": f'["MONTHS:{month}.{year}"]',
        "pok": "1"
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.post(
            "http://stat.gibdd.ru/map/getMainMapData",
            json=rf_payload,
            headers=headers,
            timeout=30
        )
        
        if response.status_code != 200:
            logger.error(f"Ошибка: {response.status_code}")
            return
        
        result = response.json()
        metabase = json.loads(result["metabase"])
        maps_data = json.loads(metabase[0]["maps"])
        
        regions = []
        for region in maps_data:
            regions.append({
                "id": region["id"],
                "name": region["name"],
                "districts": []
            })
        
        logger.info(f"Найдено {len(regions)} регионов")
              
        for i, region in enumerate(regions, 1):
            print(f"[{i}/{len(regions)}] {region['name']} ({region['id']})...")
            
            region_payload = {
                "maptype": 1,
                "region": region["id"],
                "date": f'["MONTHS:{month}.{year}"]',
                "pok": "1"
            }
            
            try:
                reg_response = requests.post(
                    "http://stat.gibdd.ru/map/getMainMapData",
                    json=region_payload,
                    headers=headers,
                    timeout=30
                )
                
                if reg_response.status_code == 200:
                    reg_result = reg_response.json()
                    reg_metabase = json.loads(reg_result["metabase"])
                    reg_maps_data = json.loads(reg_metabase[0]["maps"])
                    
                    municipalities = []
                    for municipality in reg_maps_data:
                        municipalities.append({
                            "id": municipality["id"],
                            "name": municipality["name"]
                        })
                    
                    region["districts"] = municipalities
                    logger.info(f"найдено {len(municipalities)} муниципалитетов")
                else:
                    logger.error(f"ошибка {reg_response.status_code}")
                    
            except Exception as e:
                logger.error(f"ошибка: {e}")
            
            if i < len(regions):
                time.sleep(0.5)
        
        filename = "regions_all.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(regions, f, ensure_ascii=False, indent=2)
        
        total_municipalities = sum(len(r["districts"]) for r in regions)
        logger.info(f"Файл: {filename}")
        logger.info(f"Регионов: {len(regions)}")
        logger.info(f"Всего муниципалитетов: {total_municipalities}")
                
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")

if __name__ == "__main__":  
    get_all_regions()

districts_id = pd.read_json('regions_all.json')

# ШАГ 1: Explode
# Разворачиваем список в строки. Индексы продублируются.
df_exploded = districts_id.explode('districts')

# Сбрасываем индекс, чтобы потом корректно склеить колонки
df_exploded = df_exploded.reset_index(drop=True)

# ШАГ 2: Превращаем словари в колонки
# pd.json_normalize делает из колонки со словарями красивую таблицу
normalized_data = pd.json_normalize(df_exploded['districts'])

# ШАГ 3: Объединяем старые данные с новыми
# Удаляем старую колонку 'uchInfo' и приклеиваем новые
districts = pd.concat([df_exploded.drop(columns=['districts']), normalized_data], axis=1)

districts.columns = ['region_id', 'region', 'district_id', 'district']

districts['district'] = (districts['district'].str.replace('г.', '')
                         .str.replace('ГО', '')
                         .str.replace('ский район', '')
                         .str.replace('ий район', '')
                         .str.strip())

districts['region_id'] = districts['region_id'].astype('str')

districts = districts[~districts['district_id'].isna()]

load_data_to_database(districts, 'districts_id')