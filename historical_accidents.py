import json
import requests
import pandas as pd
import time
import numpy as np
import datetime
from supabase import create_client
import os

def get_dtp_cards(region_id, district_id, year, month, start=1, end=10000):
    
    """Получение полных карточек ДТП с пагинацией"""
    
    url = "http://stat.gibdd.ru/map/getDTPCardData"

    payload = {
        "data": {
            "date": [f"MONTHS:{month}.{year}"],
            "ParReg": region_id,
            "order": {"type": "1", "fieldName": "dat"},
            "reg": district_id,
            "ind": "1",
            "st": str(start),
            "en": str(end),
            "fil": {"isSummary": False}, # Полные данные вместо сводных},
            "fieldNames": [
                "dat", "time", "coordinates", "infoDtp", "k_ul", "dor", "ndu",
                "k_ts", "ts_info", "pdop", "pog", "osv", "s_pch", "s_pog",
                "n_p", "n_pg", "obst", "sdor", "t_osv", "t_p", "t_s", "v_p", "v_v"
            ]
        }
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    try:
        # Двойное кодирование JSON требуется API ГИБДД
        request_data = {
            "data": json.dumps(payload["data"], separators=(',', ':'))
        }

        response = requests.post(
            url,
            json=request_data,
            headers=headers,
            timeout=3
        )

        if response.status_code == 200:
            response_data = json.loads(response.text)
            data = json.loads(response_data["data"]).get("tab", [])
            return pd.DataFrame(data)
        else:
            print(f"Ошибка HTTP: {response.status_code}")
            return None

    except Exception as e:
        print(f"Ошибка при запросе данных: {str(e)}", end=' ')
        return None


def get_dtp_by_city(region_id, district_id, region, city):

    ''' 
    По данным ОКАТО для соответствующего города возвращается датафрейм с данными о ДТП,
    начиная с 2015 года и по текущий день
    ''' 

    dtp_data = pd.DataFrame()
    today_date = datetime.date.today()
    
    for year in range(2015, today_date.year + 1):
        for month in range(1, 13):

            if year == today_date.year and month == today_date.month:
                break
            
            temp_df = get_dtp_cards(region_id, district_id, year, month)
        
            if temp_df is not None:
                dtp_data = pd.concat([dtp_data, temp_df], ignore_index=True)
                print(year, month, end='  ->  ')  

            else:
                print(f'за {year} {month} нет данных', end='  ->  ')
                
            time.sleep(2)
            

    dtp_data['region'] = region
    dtp_data['city'] = city
    
    return dtp_data



def normalization_table(dtp_info):

    info_data, vehicles, participants = [], [], []

    for dtp in dtp_info:
        dtp_id = dtp['dtp_id']
        info_data.append({key: value for key, value in dtp.items() if key not in ['ts_info', 'uchInfo']})

        for uch in dtp['uchInfo']:
            new_uch = {key: value for key, value in uch.items()}
            new_uch['dtp_id'] = dtp_id
            participants.append(new_uch)

        for ts in dtp['ts_info']:
            new_ts = {key: value for key, value in ts.items() if key != 'ts_uch'}
            new_ts['dtp_id'] = dtp_id
            vehicles.append(new_ts)
            n_ts = new_ts['n_ts'] # фиксирую, к какому транспортному средству относятся участники
            
            for uch in ts['ts_uch']:
                new_uch = {key: value for key, value in uch.items()}
                new_uch['dtp_id'] = dtp_id
                new_uch['n_ts'] = n_ts # записываю номер транспортного средства
                participants.append(new_uch)

    return pd.DataFrame(info_data), pd.DataFrame(vehicles), pd.DataFrame(participants)


def update_dict(row):
    
    ''' Функция добавляет уникальный идентификатор ДТП - KartId в словарь
        для нормализации таблиц, для дальнейшего их связывания'''
    
    row['infoDtp']['dtp_id'] = row['KartId']
    
    return row['infoDtp']

def rename_columns_dtp(dtp, dtp_info, vehicles, participants):
    
    dtp = dtp.rename(columns={
        'KartId': 'dtp_id',
        'rowNum': 'row_num',
        'Time': 'time',
        'District': 'district',
        'DTP_V': 'dtp_type',
        'POG': 'death_count',
        'RAN': 'injured_count',
        'K_TS': 'vehicles_count',
        'K_UCH': 'participants_count',
        'emtp_number': 'emergency_num',
        'infoDtp': 'dtp_info'
    })
    
    
    dtp_info = dtp_info.rename(columns={
        'ndu': 'road_disadvantages',
        'dor': 'road',
        'sdor': 'road_type',
        'n_p': 'city',
        'k_ul': 'street_category',
        'dor_k': 'road_category', 
        'dor_z': 'road_sign', 
        'factor': 'contributing_factors',
        's_pog': 'weather',
        's_pch': 'road_surface_condition',
        'osv': 'lighting',
        's_dtp': 'dtp_scheme',
        'COORD_W': 'lat',
        'COORD_L': 'lon',
        'OBJ_DTP': 'nearby_objects', 
        'uchInfo': 'participants_info', 
        'ts_info': 'vehicles_info'   
    })
    
    
    vehicles = vehicles.rename(columns={
        'n_ts': 'vehicles_num', 
        'ts_s': 'vehicles_fled', 
        't_ts': 'vehicle_type', 
        'marka_ts': 'vehicle_brand', 
        'm_ts': 'vehicle_model', 
        'r_rul': 'drive_type', 
        'g_v': 'manufacture_year',
        'm_pov': 'damage_location',
        't_n': 'technical_defects', 
        'f_sob': 'ownership_type', 
        'o_pf':'org_legal_form'    
    })
    
    participants = participants.rename(columns={
        'K_UCH':'participant_role',
        'NPDD':'traffic_violations',
        'S_T':'injury_severity',
        'POL':'sex',
        'V_ST':'driving_experience_years',
        'ALCO':'alco',
        'SOP_NPDD':'other_traffic_violations',
        'SAFETY_BELT':'safety_belt',
        'S_SM':'participant_fled',
        'N_UCH':'participant_num',
        'S_SEAT_GROUP':'seat_position',
        'INJURED_CARD_ID':'injured_card_id',
        'n_ts':'vehicles_num'
    })
    
    return dtp, dtp_info, vehicles, participants


def preprocess_dtp(dtp):
    
    dtp = dtp.astype({
        'death_count': 'int',
        'injured_count': 'int',
        'vehicles_count': 'int',
        'participants_count': 'int'
    })
    
    dtp['dtp_date'] = dtp['date'] + ' ' + dtp['time']
    dtp['dtp_date'] = pd.to_datetime(dtp['dtp_date'], dayfirst=True, errors='coerce')
    
    dtp = dtp.drop(['date', 'time'], axis=1) 
    dtp['district'] = dtp['district'].replace('Ленинский Екб', 'Ленинский')
    
    return dtp



def preprocess_dtp_info(dtp_info):
    
    dtp_info = dtp_info.astype({
        'lat': 'float',
        'lon': 'float'
    })

    categorical_col = ['city', 'street', 'house', 'road', 'street_category', 'road_category',
                       'road_sign', 'dtp_scheme', 'road_surface_condition', 'lighting', 'change_org_motion', 'km', 'm']
    dtp_info[categorical_col] = dtp_info[categorical_col].replace('', np.nan)

    dtp_info[['road_sign', 'road_surface_condition']] = (dtp_info[['road_sign', 'road_surface_condition']]
                                                         .replace({'Не указано': np.nan, 
                                                                   'Не установлено': np.nan}))

    return dtp_info


def preprocess_dtp_vehicles(vehicles):
    
    vehicles['vehicles_num'] = vehicles['vehicles_num'].astype('int')

    vehicles['vehicles_fled'] = vehicles['vehicles_fled'].replace({'Нет': 'Осталось на месте ДТП',
                                                                   'Да': 'Скрылось с места ДТП'})

    categorical_col = ['vehicles_fled', 'vehicle_type', 'vehicle_brand', 'vehicle_model', 'color', 'drive_type',
                       'manufacture_year', 'ownership_type', 'org_legal_form']
    vehicles[categorical_col] = vehicles[categorical_col].replace('', np.nan)

    return vehicles


def preprocess_dtp_participants(participants):
    
    participants = participants.astype({
        'participant_num': 'int'})

    categorical_col = ['participant_role', 'injury_severity', 'driving_experience_years', 
                       'alco', 'seat_position', 'sex']
    participants[categorical_col] = participants[categorical_col].replace('', np.nan)

    participants['sex'] = participants['sex'].replace('Не определен', np.nan)
    
    participants = participants.drop(['injured_card_id'], axis=1) 
    
    return participants


def prepare_data_for_database(dtp_data):
    
    dtp_data['infoDtp'] = dtp_data.apply(update_dict, axis=1)
    
    dtp_info, vehicles, participants = normalization_table(dtp_data['infoDtp'])
    dtp = dtp_data.drop('infoDtp', axis=1)

    dtp, dtp_info, vehicles, participants = rename_columns_dtp(dtp, dtp_info, vehicles, participants)

    dtp = preprocess_dtp(dtp)
    dtp_info = preprocess_dtp_info(dtp_info)
    vehicles = preprocess_dtp_vehicles(vehicles)
    participants =  preprocess_dtp_participants(participants)

    return dtp, dtp_info, vehicles, participants


def load_dtp_data_to_database(df, table_name):

    project_url = 'https://lpdaqqydnpvynwymzxxt.supabase.co'
    api_key = ...

    supabase = create_client(project_url, api_key)

    json_string = df.to_json(orient='records', date_format='iso')
    data_to_insert = json.loads(json_string)

    try:
        response = supabase.table(table_name).insert(data_to_insert).execute()
        print("Успешно загружено!")
        
    except Exception as e:
        print(f"Ошибка: {e}")


dtp_data_ekb = get_dtp_by_city('65', '654011', 'Свердловская область', 'Екатеринбург')
dtp, dtp_info, vehicles, participants = prepare_data_for_database(dtp_data_ekb)
load_dtp_data_to_database(dtp, 'dtp')
load_dtp_data_to_database(dtp_info, 'dtp_info')
load_dtp_data_to_database(vehicles, 'vehicles')
load_dtp_data_to_database(participants, 'participants')


dtp_data_perm = get_dtp_by_city('57', '86037', 'Пермский край', 'Пермь')
dtp, dtp_info, vehicles, participants = prepare_data_for_database(dtp_data_perm)
load_dtp_data_to_database(dtp, 'dtp')
load_dtp_data_to_database(dtp_info, 'dtp_info')
load_dtp_data_to_database(vehicles, 'vehicles')
load_dtp_data_to_database(participants, 'participants')





