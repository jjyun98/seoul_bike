# /opt/airflow/plugins/rent_utils.py

import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime
import boto3
from io import BytesIO


def get_rent_history_by_day(api_key, year, month, day):
    """지정된 날짜의 모든 대여 이력을 시간대별로 수집"""
    all_data = []
    date_str = f"{year}-{month:02d}-{day:02d}"
    
    print(f"[{date_str}] 데이터 수집 시작")
    
    for hour in range(24):
        try:
            # 시간대별 전체 데이터 건수 확인
            check_url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/tbCycleRentData/1/5/{date_str}/{hour}"
            response = requests.get(check_url, timeout=20)
            response.raise_for_status()
            data = response.json()
            
            if 'rentData' not in data or 'list_total_count' not in data['rentData']:
                continue
            
            total_count = int(data['rentData']['list_total_count'])
            if total_count == 0:
                continue

            # 1000개씩 페이징하여 수집
            for start_idx in range(1, total_count + 1, 1000):
                end_idx = min(start_idx + 999, total_count)
                page_url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/tbCycleRentData/{start_idx}/{end_idx}/{date_str}/{hour}"
                
                page_response = requests.get(page_url, timeout=20)
                page_data = page_response.json()
                
                if 'rentData' in page_data and 'row' in page_data['rentData']:
                    rows = page_data['rentData']['row']
                    all_data.extend(rows)
                
                time.sleep(0.1)  # API 부하 방지
                
        except Exception as e:
            print(f"[{date_str} {hour:02d}시] 수집 실패: {e}")
            continue
            
    print(f"[{date_str}] 총 {len(all_data)}건 수집")
    return pd.DataFrame(all_data)


def load_valid_stations(bucket_name):
    """S3에서 유효한 정거장 목록을 로드"""
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key='station/station.csv')
    df_station = pd.read_csv(BytesIO(obj['Body'].read()))
    
    valid_stations = set(df_station['station_id'])
    print(f"유효 정거장 {len(valid_stations)}개 로드 완료")
    return valid_stations


def upload_to_s3(df, bucket_name, file_path):
    """데이터프레임을 Parquet으로 S3에 업로드"""
    if df.empty:
        print(f"데이터 없음 - 업로드 스킵: {file_path}")
        return

    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow', compression='snappy')
    buffer.seek(0)
    
    s3 = boto3.client('s3')
    s3.put_object(Bucket=bucket_name, Key=file_path, Body=buffer.getvalue())
    print(f"업로드 완료: s3://{bucket_name}/{file_path} ({len(df):,}건)")


def standardize_columns(df):
    """컬럼명을 표준 형식으로 변환 (구/신 API 스키마 대응)"""
    # 구 스키마 (한글 컬럼명)
    if '대여일시' in df.columns:
        column_mapping = {
            '대여일시': 'rent_datetime',
            '반납일시': 'return_datetime',
            '이용거리(M)': 'distance',
            '생년': 'birth_year',
            '성별': 'gender',
            '이용자종류': 'user_type',
            '대여대여소ID': 'rent_station_id',
            '반납대여소ID': 'return_station_id',
            '자전거구분': 'bike_type'
        }
        return df.rename(columns=column_mapping)
    
    # 신 스키마 (영문 컬럼명)
    elif 'RENT_DT' in df.columns:
        column_mapping = {
            'RENT_DT': 'rent_datetime',
            'RTN_DT': 'return_datetime',
            'USE_DST': 'distance',
            'BIRTH_YEAR': 'birth_year',
            'SEX_CD': 'gender',
            'USR_CLS_CD': 'user_type',
            'RENT_STATION_ID': 'rent_station_id',
            'RETURN_STATION_ID': 'return_station_id',
            'BIKE_SE_CD': 'bike_type'
        }
        return df.rename(columns=column_mapping)
    
    return df


def preprocess_rent_data(df, valid_stations):
    """대여 이력 데이터 전처리"""
    
    # 1. 컬럼명 표준화
    df = standardize_columns(df)
    
    # 2. 필수 컬럼 확인
    required = ['rent_datetime', 'return_datetime', 'rent_station_id', 'return_station_id']
    if not all(col in df.columns for col in required):
        print(f"필수 컬럼 부족 - 스킵")
        return pd.DataFrame()

    # 3. 불필요한 컬럼 제거
    drop_cols = [
        '대여 대여소명', '반납대여소명', '자전거번호', '대여거치대', '반납거치대',
        '이용시간(분)', '반납대여소번호', '대여 대여소번호',
        'RENT_ID', 'RENT_NM', 'RENT_HOLD', 'RTN_ID', 'RTN_NM', 'RTN_HOLD',
        'USE_MIN', 'BIKE_ID', 'START_INDEX', 'END_INDEX', 'RNUM'
    ]
    df = df.drop(columns=[col for col in drop_cols if col in df.columns], errors='ignore')

    # 4. 결측값 처리
    df = df.replace('\\N', np.nan)

    # 5. 타입 변환
    df['rent_datetime'] = pd.to_datetime(df['rent_datetime'], errors='coerce')
    df['return_datetime'] = pd.to_datetime(df['return_datetime'], errors='coerce')
    
    if 'birth_year' in df.columns:
        df['birth_year'] = pd.to_numeric(df['birth_year'], errors='coerce', downcast='unsigned')
    
    if 'distance' in df.columns:
        df['distance'] = pd.to_numeric(df['distance'], errors='coerce', downcast='float')
    
    # 메모리 최적화 - 범주형 변환
    for col in ['gender', 'user_type', 'bike_type']:
        if col in df.columns:
            df[col] = df[col].astype('category')
    
    # 6. 필수값 누락 제거
    df = df.dropna(subset=required)

    # 7. 유효하지 않은 정거장 제거
    df = df[
        df['rent_station_id'].isin(valid_stations) &
        df['return_station_id'].isin(valid_stations)
    ]

    # 8. 이상치 제거
    if 'distance' in df.columns:
        df = df[df['distance'] >= 0]
    
    if 'birth_year' in df.columns:
        current_year = datetime.now().year
        df = df[
            df['birth_year'].isna() |
            ((df['birth_year'] >= 1920) & (df['birth_year'] <= current_year))
        ]
    
    print(f"  전처리 후: {len(df):,}건")
    return df


def add_primary_key(df):
    """대여 ID 생성 (YYYYMMDDHH + 6자리 순번)"""
    if df.empty:
        return df
    
    df = df.sort_values('rent_datetime').reset_index(drop=True)
    
    # 시간대별 그룹화 키 생성
    hour_key = df['rent_datetime'].dt.strftime('%Y%m%d%H')
    
    # 시간대별 순번 생성
    df['rental_id'] = (
        hour_key + 
        df.groupby(hour_key).cumcount().add(1).astype(str).str.zfill(6)
    )
    
    # rental_id를 첫 번째 컬럼으로 이동
    cols = ['rental_id'] + [col for col in df.columns if col != 'rental_id']
    return df[cols]


def process_weekly_data(api_key, bucket, year, month, start_day, end_day, valid_stations):
    """주간 단위로 데이터를 수집하고 처리"""
    
    print(f"[{year}-{month:02d}] {start_day}~{end_day}일 처리 중")
    
    weekly_data = []
    
    # 일별 데이터 수집
    for day in range(start_day, end_day + 1):
        try:
            df_day = get_rent_history_by_day(api_key, year, month, day)
            if not df_day.empty:
                df_processed = preprocess_rent_data(df_day, valid_stations)
                if not df_processed.empty:
                    weekly_data.append(df_processed)
        except Exception as e:
            print(f"[{year}-{month:02d}-{day:02d}] 처리 실패: {e}")
            continue
    
    if not weekly_data:
        return None
    
    # 주간 데이터 병합
    df_week = pd.concat(weekly_data, ignore_index=True)
    df_week = add_primary_key(df_week)
    
    print(f"[{year}-{month:02d}] {start_day}~{end_day}일 완료: {len(df_week):,}건")
    return df_week


def merge_weekly_to_monthly(bucket, year, month, total_weeks):
    """주간 임시 파일들을 월 단위로 병합"""
    
    print(f"[{year}-{month:02d}] 주간 파일 병합 시작 (총 {total_weeks}주)")
    
    s3 = boto3.client('s3')
    weekly_files = []
    
    # 주간 파일 로드
    for week in range(1, total_weeks + 1):
        temp_key = f"rent/{year}/temp/month_{month:02d}_week_{week}.parquet"
        try:
            obj = s3.get_object(Bucket=bucket, Key=temp_key)
            df_week = pd.read_parquet(BytesIO(obj['Body'].read()))
            weekly_files.append(df_week)
            print(f"  Week {week}: {len(df_week):,}건 로드")
        except Exception as e:
            print(f"  Week {week} 로드 실패: {e}")
            continue
    
    if not weekly_files:
        print(f"[{year}-{month:02d}] 병합할 데이터 없음")
        return
    
    # 전체 병합
    df_month = pd.concat(weekly_files, ignore_index=True)
    
    # 시간순 정렬 후 PK 재생성
    df_month = df_month.sort_values('rent_datetime').reset_index(drop=True)
    hour_key = df_month['rent_datetime'].dt.strftime('%Y%m%d%H')
    df_month['rental_id'] = (
        hour_key + 
        df_month.groupby(hour_key).cumcount().add(1).astype(str).str.zfill(6)
    )
    
    # 컬럼 순서 조정
    cols = ['rental_id'] + [col for col in df_month.columns if col != 'rental_id']
    df_month = df_month[cols]
    
    # 최종 파일 저장
    final_path = f"rent/{year}/month_{month:02d}.parquet"
    upload_to_s3(df_month, bucket, final_path)
    
    print(f"[{year}-{month:02d}] 최종 병합 완료: {len(df_month):,}건")
    
    # 임시 파일 삭제
    for week in range(1, total_weeks + 1):
        temp_key = f"rent/{year}/temp/month_{month:02d}_week_{week}.parquet"
        try:
            s3.delete_object(Bucket=bucket, Key=temp_key)
        except:
            pass


def collect_and_upload_monthly_data(api_key, bucket, year, month):
    """월별 대여 이력 수집 및 업로드 (주간 분할 처리)"""
    
    print(f"\n{'='*60}")
    print(f"{year}년 {month}월 처리 시작")
    print(f"{'='*60}")
    
    # 유효 정거장 로드
    valid_stations = load_valid_stations(bucket)
    
    # 해당 월의 일수 계산
    days_in_month = pd.Period(f'{year}-{month}').days_in_month
    
    # 주간 단위로 분할 처리
    week_size = 7
    week_num = 0
    
    for start_day in range(1, days_in_month + 1, week_size):
        end_day = min(start_day + week_size - 1, days_in_month)
        week_num += 1
        
        # 주간 데이터 처리
        df_week = process_weekly_data(
            api_key, bucket, year, month, 
            start_day, end_day, valid_stations
        )
        
        if df_week is None:
            continue
        
        # 주간 임시 파일 저장
        temp_path = f"rent/{year}/temp/month_{month:02d}_week_{week_num}.parquet"
        upload_to_s3(df_week, bucket, temp_path)
        
        # 메모리 해제
        del df_week
    
    # 주간 파일들을 월 단위로 병합
    merge_weekly_to_monthly(bucket, year, month, week_num)
    
    print(f"\n{year}년 {month}월 처리 완료\n")


def process_entire_year(api_key, bucket, year):
    """연도별 전체 월 데이터 순차 처리"""
    
    print(f"\n{'#'*60}")
    print(f"# {year}년 연간 데이터 처리 시작")
    print(f"{'#'*60}\n")
    
    for month in range(1, 13):
        try:
            collect_and_upload_monthly_data(api_key, bucket, year, month)
        except Exception as e:
            print(f"\n[ERROR] {year}년 {month}월 처리 실패: {e}")
            raise
    
    print(f"\n{'#'*60}")
    print(f"# {year}년 연간 데이터 처리 완료")
    print(f"{'#'*60}\n")