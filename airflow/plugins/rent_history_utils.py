import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime
import boto3
from io import BytesIO

# --- Core Functions ---

def get_rent_history_by_day(api_key, year, month, day):
    """지정된 날짜의 모든 대여 이력을 시간대별/청크별로 수집합니다."""
    all_data = []
    date_str = f"{year}-{month:02d}-{day:02d}"
    
    print(f"[{date_str}] 데이터 수집 시작")
    
    for hour in range(24):
        try:
            # 1. 해당 시간대의 전체 데이터 수 확인
            url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/tbCycleRentData/1/5/{date_str}/{hour}"
            response = requests.get(url, timeout=20)
            response.raise_for_status()
            
            data = response.json()
            
            if 'rentData' not in data or 'list_total_count' not in data['rentData']:
                continue
            
            total_count = int(data['rentData']['list_total_count'])
            if total_count == 0:
                continue

            # 2. 1000개 단위로 페이징하여 데이터 수집
            for start in range(1, total_count + 1, 1000):
                end = min(start + 999, total_count)
                page_url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/tbCycleRentData/{start}/{end}/{date_str}/{hour}"
                
                page_response = requests.get(page_url, timeout=20)
                page_data = page_response.json()
                
                if 'rentData' in page_data and 'row' in page_data['rentData']:
                    rows = page_data['rentData']['row']
                    all_data.extend(rows)
                
                time.sleep(0.1) # API 부하 감소
                
        except requests.exceptions.RequestException as e:
            print(f"!! [{date_str} {hour:02d}시] API 요청 실패: {e}")
            continue
        except Exception as e:
            print(f"!! [{date_str} {hour:02d}시] 데이터 처리 오류: {e}")
            continue
            
    print(f"[{date_str}] 총 {len(all_data)}건 수집 완료")
    return pd.DataFrame(all_data)


def load_valid_stations(bucket_name):
    """S3에서 유효한 정거장 ID 목록을 로드합니다."""
    try:
        s3 = boto3.client('s3')
        key = 'station/station.csv'
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        df_station = pd.read_csv(BytesIO(obj['Body'].read()))
        
        valid_stations = set(df_station['station_id'])
        print(f"✅ 유효 정거장 {len(valid_stations)}개 로드 완료 (from s3://{bucket_name}/{key})")
        return valid_stations
    except Exception as e:
        print(f"🛑 S3에서 정거장 정보 로드 실패: {e}")
        raise


def upload_parquet_to_s3(df, bucket_name, file_path):
    """DataFrame을 Parquet 형식으로 S3에 업로드합니다."""
    if df.empty:
        print(f"⚠️ 업로드할 데이터가 없어 s3://{bucket_name}/{file_path} 생성을 건너뜁니다.")
        return

    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)
    
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=bucket_name,
        Key=file_path,
        Body=parquet_buffer.getvalue()
    )
    print(f"✅ 업로드 완료: s3://{bucket_name}/{file_path} ({len(df)}건)")


# --- Main Orchestration Function ---

def collect_and_upload_monthly_data(api_key, bucket, year, month):
    """
    월별 대여 이력을 수집, 전처리하여 S3에 업로드합니다.
    메모리 관리를 위해 일별로 데이터를 처리하고 마지막에 병합합니다.
    """
    print(f"🚀 ===== {year}년 {month}월 대여 이력 처리 시작 ====")
    
    # 1. 유효 정거장 정보 로드 (작업 시작 시 한 번만)
    valid_stations = load_valid_stations(bucket)
    
    # 2. 해당 월의 모든 날짜에 대해 데이터 수집 및 전처리
    processed_daily_dfs = []
    days_in_month = pd.Period(f'{year}-{month}').days_in_month
    
    for day in range(1, days_in_month + 1):
        try:
            # 일별 데이터 수집
            df_day = get_rent_history_by_day(api_key, year, month, day)
            if df_day.empty:
                continue
            
            # 일별 데이터 전처리 (PK 생성 제외)
            df_processed = _preprocess_rent_df(df_day, valid_stations)
            if not df_processed.empty:
                processed_daily_dfs.append(df_processed)
                
        except Exception as e:
            print(f"!! {year}-{month}-{day} 처리 중 심각한 오류 발생: {e}")
            continue
            
    if not processed_daily_dfs:
        print(f"🛑 {year}년 {month}월에 처리할 데이터가 없습니다. 작업을 종료합니다.")
        return

    # 3. 모든 일별 데이터를 하나로 병합
    print("\n🔄 모든 일별 데이터 병합 및 최종 처리 중...")
    df_month = pd.concat(processed_daily_dfs, ignore_index=True)
    print(f"월 총 데이터: {len(df_month)}건")
    
    # 4. PK 생성 및 최종 정리
    df_final = _finalize_rent_df(df_month)

    # 5. S3에 Parquet으로 업로드
    file_path = f"rent/{year}/month_{month:02d}.parquet"
    upload_parquet_to_s3(df_final, bucket, file_path)
    
    print(f"🎉 ===== {year}년 {month}월 대여 이력 처리 완료 ====")


# --- Helper Functions for Preprocessing ---

def _preprocess_rent_df(df, valid_stations):
    """대여 이력 데이터프레임의 기본 전처리 (PK 생성 제외)"""
    
    # 1. 스키마 확인 및 컬럼명 표준화
    if '대여일시' in df.columns: # 구 스키마 (한글 컬럼명)
        df = df.rename(columns={
            '대여일시': 'rent_datetime', '반납일시': 'return_datetime',
            '이용거리(M)': 'distance', '생년': 'birth_year', '성별': 'gender',
            '이용자종류': 'user_type', '대여대여소ID': 'rent_station_id',
            '반납대여소ID': 'return_station_id', '자전거구분': 'bike_type'
        })
    elif 'RENT_DT' in df.columns: # 신 스키마 (영문 컬럼명)
        df = df.rename(columns={
            'RENT_DT': 'rent_datetime', 'RTN_DT': 'return_datetime',
            'USE_DST': 'distance', 'BIRTH_YEAR': 'birth_year',
            'SEX_CD': 'gender', 'USR_CLS_CD': 'user_type',
            'RENT_STATION_ID': 'rent_station_id', 'RETURN_STATION_ID': 'return_station_id',
            'BIKE_SE_CD': 'bike_type'
        })
    else:
        print(f"⚠️ 컬럼 스키마 불일치. 건너뜁니다. 현재 컬럼: {df.columns.tolist()}")
        return pd.DataFrame()

    # 2. 필수 컬럼 존재 여부 확인 (표준화된 이름으로)
    required_cols = ['rent_datetime', 'return_datetime', 'rent_station_id', 'return_station_id']
    if not all(col in df.columns for col in required_cols):
        print(f"⚠️ 표준화 후 필수 컬럼 부족. 건너뜁니다. 현재 컬럼: {df.columns.tolist()}")
        return pd.DataFrame()

    # 3. 불필요한 컬럼 제거 (존재할 수 있는 원본 컬럼들)
    cols_to_drop = [
        '대여 대여소명', '반납대여소명', '자전거번호', '대여거치대', '반납거치대', 
        '이용시간(분)', '반납대여소번호', '대여 대여소번호', 'RENT_ID', 'RENT_NM', 
        'RENT_HOLD', 'RTN_ID', 'RTN_NM', 'RTN_HOLD', 'USE_MIN', 'BIKE_ID',
        'START_INDEX', 'END_INDEX', 'RNUM'
    ]
    df = df.drop(columns=[col for col in cols_to_drop if col in df.columns], errors='ignore')

    # 4. '\N' -> NaN
    df = df.replace('\\N', np.nan)

    # 5. 타입 변환 (표준화된 이름으로)
    df['rent_datetime'] = pd.to_datetime(df['rent_datetime'], errors='coerce')
    df['return_datetime'] = pd.to_datetime(df['return_datetime'], errors='coerce')
    if 'birth_year' in df.columns:
        df['birth_year'] = pd.to_numeric(df['birth_year'], errors='coerce')
    if 'distance' in df.columns:
        df['distance'] = pd.to_numeric(df['distance'], errors='coerce')
    
    # 6. 필수 컬럼 누락 데이터 제거
    df.dropna(subset=['rent_datetime', 'return_datetime', 'rent_station_id', 'return_station_id'], inplace=True)

    # 7. 외래키 무결성 체크
    df = df[
        df['rent_station_id'].isin(valid_stations) &
        df['return_station_id'].isin(valid_stations)
    ]

    # 8. 이상치 제거
    if 'distance' in df.columns:
        df['distance'] = pd.to_numeric(df['distance'], errors='coerce').fillna(0)
        df = df[df['distance'] >= 0]
    
    if 'birth_year' in df.columns:
        df = df[
            (df['birth_year'].isna()) |
            ((df['birth_year'] >= 1920) & (df['birth_year'] <= datetime.now().year))
        ]
        
    print(f"기본 전처리 후 {len(df)}건")
    return df

def _finalize_rent_df(df):
    """전처리된 데이터프레임에 PK를 생성하고 컬럼 순서를 정렬"""
    if df.empty:
        return df

    df = df.sort_values(by='rent_datetime').reset_index(drop=True)
    
    # PK 추가: 'YYYYMMDDHH' + 시간별 순번 (6자리)
    df['rental_id'] = (
        df['rent_datetime'].dt.strftime('%Y%m%d%H') +
        df.groupby(df['rent_datetime'].dt.strftime('%Y%m%d%H')).cumcount().add(1).astype(str).str.zfill(6)
    )

    # 맨 앞으로 이동
    cols = ['rental_id'] + [col for col in df.columns if col != 'rental_id']
    df = df[cols]
    
    print(f"최종 처리 후 {len(df)}건 (PK 생성 완료)")
    return df

# --- [Optional] Test function for single day ---
def collect_and_upload_day_for_test(api_key, bucket, year, month, day):
    """(테스트용) 하루치 데이터를 수집, 전처리하여 S3에 업로드"""
    print(f"🚀 ===== {year}-{month}-{day} 테스트 시작 ====")
    valid_stations = load_valid_stations(bucket)
    df_day = get_rent_history_by_day(api_key, year, month, day)
    if not df_day.empty:
        df_processed = _preprocess_rent_df(df_day, valid_stations)
        df_final = _finalize_rent_df(df_processed)
        file_path = f"rent/test/{year}{month:02d}{day:02d}.parquet"
        upload_parquet_to_s3(df_final, bucket, file_path)
    print(f"🎉 ===== 테스트 종료 =====")


def process_entire_year(api_key, bucket, year):
    """지정된 연도의 1월부터 12월까지 모든 월별 데이터를 순차적으로 처리합니다."""
    print(f"🚀🚀🚀 ===== {year}년 전체 대여 이력 처리 시작 ===== 🚀🚀🚀")
    for month in range(1, 13):
        try:
            collect_and_upload_monthly_data(api_key, bucket, year, month)
        except Exception as e:
            print(f"🛑🛑🛑 {year}년 {month}월 처리 중 심각한 오류 발생으로 {year}년 작업을 중단합니다: {e}")
            # 한 해의 특정 월에서 실패하면 해당 연도 전체를 실패로 간주하고 예외를 다시 발생시킴
            raise
    print(f"🎉🎉🎉 ===== {year}년 전체 대여 이력 처리 완료 ===== 🎉🎉🎉")
