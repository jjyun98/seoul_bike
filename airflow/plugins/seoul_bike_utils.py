import requests
import pandas as pd
from datetime import datetime
import boto3
from io import StringIO


def fetch_seoul_bike_stations(api_key):
    """서울 따릉이 정거장 정보 수집"""
    
    url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/tbCycleStationInfo/1/1/"
    response = requests.get(url)
    total_count = int(response.json()['stationInfo']['list_total_count'])
    
    all_data = []
    for start in range(1, total_count + 1, 1000):
        end = min(start + 999, total_count)
        url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/tbCycleStationInfo/{start}/{end}/"
        
        response = requests.get(url)
        data = response.json()['stationInfo']['row']
        all_data.extend(data)
    
    return pd.DataFrame(all_data)


def preprocess_station_data(df):
    """정거장 데이터 전처리"""
    
    df = df.drop(['RENT_ID_NM', 'HOLD_NUM', 'STA_ADD1', 'STA_ADD2', 
                  'START_INDEX', 'END_INDEX', 'RNUM', 'RENT_NO'], axis=1)
    
    df['STA_LAT'] = df['STA_LAT'].astype(float).round(5)
    df['STA_LONG'] = df['STA_LONG'].astype(float).round(5)
    
    df = df.rename(columns={
        'STA_LOC': 'district',
        'RENT_ID': 'station_id',
        'RENT_NM': 'station_name',
        'STA_LAT': 'latitude',
        'STA_LONG': 'longitude'
    })
    
    df = df[['station_id', 'station_name', 'district', 'latitude', 'longitude']]
    
    return df

def upload_to_s3(df, bucket_name, file_name):
    """DataFrame을 S3에 CSV로 업로드 (덮어쓰기)"""
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=csv_buffer.getvalue()
    )
    
    print(f"업로드 완료: s3://{bucket_name}/{file_name}")