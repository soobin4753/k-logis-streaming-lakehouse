# 📦 Logistics Streaming Data Pipeline

## 🚀 Overview

Kafka + Spark 기반의 **실시간 물류 데이터 파이프라인 프로젝트**

배송 이벤트를 실시간으로 생성하고  
Data Lake에 저장 → ETL → 집계 → Data Mart까지  
**end-to-end 데이터 처리 흐름을 구현**

---

## 🧱 Architecture


Producer → Kafka → Spark Streaming → Raw → ETL → Processed → Aggregation → PostgreSQL


---

## 📁 Structure


data → producer → spark → db → dags → storage


---

## ⚙️ Tech Stack

- **Streaming**: Kafka  
- **Processing**: Spark (Streaming + Batch)  
- **Storage**: Parquet (Data Lake)  
- **DB**: PostgreSQL  
- **Orchestration**: Airflow (optional)  
- **Infra**: Docker  

---

## 🔄 Data Flow

### 1. Event Generation
- Producer가 배송 이벤트 생성
- Kafka `delivery-events` topic으로 전송

### 2. Streaming Ingestion
- Spark Streaming이 Kafka 구독
- Raw Parquet 저장


/storage/raw/delivery_events/


### 3. Batch ETL
- JSON 파싱 / 타입 변환 / 중복 제거
- Processed Parquet 저장

### 4. Aggregation (Metrics)
- 배송 단위 집계 (배송시간, 지연 여부 등)

### 5. Data Mart
- 지역 / 허브 / 리스크 기준 분석 테이블 생성

---
