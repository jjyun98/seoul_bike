# Seoul Bike (따릉이) Data Analysis & Visualization

서울시 공공자전거 '따릉이' 대여 데이터를 수집, 분석하고 주요 이용 노선을 시각화하는 프로젝트입니다.

## 프로젝트 개요

이 프로젝트는 서울 열린데이터광장 API를 활용하여 따릉이 대여 이력 데이터를 수집하고, 네트워크 분석을 통해 주요 이용 노선을 추출하여 다양한 형태로 시각화합니다.

### 주요 기능

- **데이터 수집**: 서울 열린데이터광장 API를 통한 실시간 데이터 수집
  - 따릉이 정거장 정보 (위치, 좌표)
  - 대여 이력 정보 (대여/반납 시간, 이용 거리, 사용자 정보)

- **데이터 전처리**
  - 결측치 처리 및 이상치 제거
  - 외래키 무결성 검증
  - 좌표 정규화 (5자리 정밀도, 약 1.1m 오차)

- **네트워크 분석**
  - OSMnx를 활용한 서울 자전거 네트워크 다운로드
  - 자전거도로 우선 가중치 설정
  - NetworkX를 통한 최단 경로 계산
  - 주요 이용 노선 추출 및 클러스터링

- **시각화**
  - Folium을 활용한 인터랙티브 지도
  - Flowmap.blue용 데이터 생성
  - Kepler.gl용 GeoJSON/CSV 생성
  - 애니메이션 시각화 지원

- **ETL 파이프라인**
  - Apache Airflow를 통한 자동화된 데이터 수집 및 처리

## 기술 스택

### 데이터 수집 및 처리
- **Python 3.x**
- **pandas**: 데이터 전처리 및 분석
- **numpy**: 수치 연산
- **requests**: API 호출
- **python-dotenv**: 환경 변수 관리

### 네트워크 분석
- **OSMnx**: OpenStreetMap 기반 도로 네트워크 분석
- **NetworkX**: 그래프 분석 및 최단 경로 계산

### 시각화
- **folium**: 인터랙티브 지도 생성
- **matplotlib**: 기본 시각화

### 데이터 파이프라인
- **Apache Airflow**: ETL 워크플로우 관리
- **Docker**: 컨테이너 기반 환경 구성

## 프로젝트 구조

```
seoulbike/
├── preprocess.ipynb          # 메인 데이터 전처리 및 분석 노트북
├── bike_etl.ipynb            # ETL 파이프라인 개발 노트북
├── airflow/                  # Airflow 설정 및 DAG
│   ├── dags/                 # Airflow DAG 파일
│   ├── docker-compose.yaml   # Airflow Docker 설정
│   └── requirements.txt      # Airflow 의존성
├── data/                     # 원본 데이터
│   ├── bike_elevation.csv    # 정거장 고도 정보
│   └── 서울특별시 공공자전거 대여이력 정보_2502.csv
├── output/                   # 분석 결과 및 시각화 파일
│   ├── 따릉이노선도.html     # Folium 지도
│   ├── 따릉이노선도.geojson  # 노선 GeoJSON
│   ├── flowmap_*.csv         # Flowmap.blue용 데이터
│   ├── kepler_*.geojson      # Kepler.gl용 데이터
│   └── 노선_요약.csv         # 노선 통계 정보
├── cache/                    # OSMnx 캐시
├── .env                      # API 키 등 환경 변수
└── .gitignore
```

## 설치 및 실행

### 1. 환경 설정

```bash
# 저장소 클론
git clone <repository-url>
cd seoulbike

# 가상환경 생성 및 활성화
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 의존성 설치
pip install pandas numpy requests python-dotenv osmnx networkx folium matplotlib jupyter
```

### 2. API 키 설정

`.env` 파일을 생성하고 서울 열린데이터광장 API 키를 추가합니다:

```
SEOUL_API_KEY=your_api_key_here
```

API 키는 [서울 열린데이터광장](http://data.seoul.go.kr/)에서 발급받을 수 있습니다.

### 3. 데이터 분석 실행

```bash
# Jupyter Notebook 실행
jupyter notebook

# preprocess.ipynb 열기 및 실행
```

### 4. Airflow 실행 (선택사항)

```bash
cd airflow

# Docker Compose로 Airflow 실행
docker-compose up -d

# Airflow 웹 UI 접속: http://localhost:8080
```

## 주요 분석 결과

### 데이터 규모
- 따릉이 정거장: 3,202개
- 분석 샘플: 10,000건의 대여 이력
- 추출된 OD(Origin-Destination) 쌍: 8,818개
- 주요 노선: 15개 (사용 빈도 상위 90% 기준)

### 네트워크 분석
- 서울 자전거 네트워크: 118,856개 노드, 314,274개 엣지
- 주요 간선: 89개 노드, 86개 엣지
- 연결된 컴포넌트: 3개

### 시각화 출력물

1. **인터랙티브 지도** (`따릉이노선도.html`)
   - 노선별 색상 구분
   - 종점 마커 표시
   - 이용 횟수 정보 툴팁

2. **Flowmap.blue 데이터**
   - `flowmap_locations.csv`: 30개 위치 정보
   - `flowmap_flows.csv`: 15개 노선 흐름 정보
   - [Flowmap.blue](https://flowmap.blue/)에서 시각화 가능

3. **Kepler.gl 데이터**
   - `kepler_따릉이노선도.geojson`: 노선 레이어
   - `kepler_animated_points.geojson`: 애니메이션 포인트
   - `kepler_complete.geojson`: 통합 시각화 데이터

## 데이터 수집 함수

### 정거장 정보 수집
```python
fetch_seoul_bike_stations(api_key)
```
- 서울시 전체 따릉이 정거장 정보를 수집합니다.
- 1000개씩 페이징 처리하여 API 제한 회피

### 대여 이력 수집
```python
get_rent_history(year)
```
- 특정 연도의 전체 대여 이력을 수집합니다.
- 날짜별, 시간별로 데이터를 수집하고 병합

## 데이터 전처리 프로세스

1. **결측치 처리**
   - `rent_station_id`, `return_station_id` 결측 제거
   - 성별, 생년 등 선택적 필드는 유지

2. **외래키 무결성**
   - 존재하지 않는 정거장 ID 참조 제거
   - 정거장 마스터 데이터와 교차 검증

3. **이상치 제거**
   - 이용 거리 < 0 제거
   - 생년 범위 검증 (1920-2010)

4. **Primary Key 생성**
   - `rental_id`: 대여일시(YYYYMMDDhh) + 순번(6자리)
   - 예: `2025020100000001`

## 노선 추출 알고리즘

1. **OD 매트릭스 생성**
   - 대여/반납 정거장 쌍별 이용 횟수 집계

2. **네트워크 가중치 설정**
   - 자전거도로: 가중치 1 (최우선)
   - 주거지역/보조간선: 가중치 2
   - 일반 차도: 가중치 3

3. **최단 경로 계산**
   - NetworkX의 `shortest_path` 알고리즘 사용
   - 가중치를 고려한 최적 경로 추출

4. **주요 간선 선택**
   - 엣지 사용 빈도 상위 90% 선택
   - 연결된 컴포넌트로 노선 그룹화

5. **노선 정의**
   - 각 컴포넌트의 가장 먼 두 지점을 종점으로 설정
   - 최소 노드 수 기준 (기본 7개) 적용

## 시각화 사용 방법

### Flowmap.blue

1. `output/flowmap_완성본.xlsx` 파일을 Google Drive에 업로드
2. Google Sheets로 열기
3. 공유 설정: "링크가 있는 모든 사용자"
4. 링크 복사
5. [Flowmap.blue](https://flowmap.blue/)에서 링크 입력

### Kepler.gl

1. [Kepler.gl](https://kepler.gl/)에서 "Add Data" 클릭
2. `output/kepler_complete.geojson` 업로드
3. 레이어 설정:
   - Arc 레이어: 노선 경로
   - Point 레이어: 애니메이션 포인트
4. 타임라인 활성화로 애니메이션 재생

## 라이선스

이 프로젝트는 교육 및 분석 목적으로 작성되었습니다.

## 데이터 출처

- [서울 열린데이터광장](http://data.seoul.go.kr/)
  - 따릉이 정거장 정보
  - 공공자전거 대여이력 정보
- [OpenStreetMap](https://www.openstreetmap.org/)
  - 서울시 도로 네트워크 데이터 (OSMnx를 통해 수집)

## 개발 환경

- Python 3.8+
- Jupyter Notebook
- macOS / Linux / Windows

## 참고 사항

- API 호출 시 0.1초 간격 권장 (Rate Limit 고려)
- OSMnx 네트워크 다운로드는 최초 1회만 수행 (캐시 활용)
- 대용량 데이터 분석 시 샘플링 권장 (메모리 최적화)
