## 📌 1. 프로젝트 개요

이 프로젝트는 물류 배송 이벤트를 실시간으로 수집하고 분석 가능한 데이터로 변환하는 파이프라인입니다.

- Seed 데이터를 기반으로 기준 데이터를 생성하고  
- Kafka를 통해 배송 이벤트를 실시간으로 수집하며  
- Spark Streaming으로 Raw 데이터를 Parquet로 저장한 뒤  
- Batch ETL을 통해 분석용 Metrics와 Data Mart를 생성합니다  

---

## 🗂️ 2. 전체 아키텍처

![Architecture](/img/logistics_architecture.png)

Seed Data 생성 (기준 데이터 초기화)  
↓  
Producer (배송/이벤트 데이터 생성)  
↓  
Kafka (실시간 이벤트 스트리밍)  
↓  
Spark Streaming (Kafka 이벤트 수집 및 Raw 저장)  
↓  
Raw Parquet (원본 이벤트 저장 레이어)  
↓  
ETL (데이터 정제 및 구조화)  
↓  
Processed Parquet (분석 가능한 형태로 변환)  
↓  
shipment_metrics (배송 단위 집계 데이터 생성)  
↓  
Data Mart (최종 분석용 집계 테이블)  

---

## 🗂️ 3. 테이블 구성

### ✅ Master  
기준 데이터로, 초기 Seed 데이터를 통해 생성되며 파이프라인 실행 시 유지됩니다.

region (지역 정보)  
hub (물류 허브 정보)  
driver (배송 기사 정보)  
vehicle (배송 차량 정보)  

---

### ✅ Transaction  
Producer 실행 시 생성되는 배송 및 배차 데이터입니다.

shipment (배송 단위 데이터)  
dispatch (배송 배차 정보)  

---

### ✅ Metrics  
배송 이벤트를 shipment 단위로 집계한 분석용 테이블입니다.

shipment_metrics  

- 배송 완료 시간  
- 지연 여부 및 지연 시간  
- 이벤트 개수  
- 평균 지연 확률 및 리스크 지표  

---

### ✅ Mart  
최종 분석 목적의 집계 테이블입니다.

mart_region_delay (지역별 지연 분석)  
mart_hub_performance (허브별 성능 분석)  
mart_risk_summary (리스크 요약 분석)  

---

## ⚙️ 4. Airflow DAG

### 📥 Ingest DAG  
데이터 수집 및 Raw 저장을 담당하는 파이프라인입니다.

logistics_ingest_pipeline  

reset_postgres_tables (기존 데이터 초기화)  
↓  
reset_storage_files (Parquet 및 체크포인트 초기화)  
↓  
start_spark_streaming (Kafka 이벤트 수집 시작)  
↓  
run_producer_1000_shipments (배송 이벤트 생성)  
↓  
wait_streaming_flush (남은 이벤트 처리 대기)  
↓  
stop_spark_streaming (Streaming 종료)  

---

### 🔄 ETL DAG  
Raw 데이터를 가공하여 Metrics 및 Mart를 생성하는 파이프라인입니다.

logistics_etl_pipeline  

reset_postgres_tables (분석 테이블 초기화)  
↓  
etl_processed_delivery_events (Raw → Processed 변환)  
↓  
build_shipment_metrics (배송 단위 지표 생성)  
↓  
build_data_mart (최종 분석 테이블 생성)  

---

## ⚡ 5. Spark 처리 구조

본 프로젝트에서는 Spark를 Streaming과 Batch 두 가지 방식으로 활용합니다.

### 🔹 Spark Streaming
Kafka로부터 배송 이벤트를 실시간으로 수집하여 Raw Parquet에 저장합니다.

- Kafka Topic 구독 (delivery_events)
- JSON 이벤트 파싱
- event_timestamp 기반 파티셔닝
- Raw Layer (/app/storage/raw) 적재

---

### 🔹 Spark Batch ETL
Raw 데이터를 정제하여 분석 가능한 형태로 변환합니다.

- 데이터 타입 변환 및 정제
- 이벤트 순서 기반 배송 흐름 재구성
- 파생 컬럼 생성 (delay, risk 등)
- Processed Parquet 생성

---

### 🔹 Metrics / Mart 생성
정제된 데이터를 기반으로 분석용 집계 테이블을 생성합니다.

- shipment_metrics: 배송 단위 KPI 계산
- mart_region_delay: 지역별 지연 분석
- mart_hub_performance: 허브 성능 분석
- mart_risk_summary: 리스크 요약 분석