# Real-Time Sales Analytics Pipeline using Kafka and Databricks

## Overview

This project implements a real-time data engineering pipeline using Apache Kafka, Confluent Cloud, Databricks Structured Streaming, Delta Lake, and the Medallion Architecture (Bronze, Silver, Gold).

The pipeline ingests streaming sales orders from Kafka, performs data cleansing and validation, and generates aggregated business metrics for analytics and reporting.

---

## Architecture

```text
Producer
    │
    ▼
Confluent Kafka Topic
    │
    ▼
Databricks Structured Streaming
    │
    ▼
Bronze Layer (Raw Data)
    │
    ▼
Silver Layer (Cleaned Data)
    │
    ▼
Gold Layer (Business Aggregations)
    │
    ▼
BI Dashboard / Analytics
```

---

## Technology Stack

| Component          | Technology                      |
| ------------------ | ------------------------------- |
| Streaming Platform | Apache Kafka (Confluent Cloud)  |
| Processing Engine  | Databricks Structured Streaming |
| Storage            | Delta Lake                      |
| Cloud Platform     | Databricks                      |
| Language           | PySpark                         |
| Architecture       | Medallion Architecture          |

---

## Data Flow

### Bronze Layer

Raw Kafka events are ingested into Delta tables.

**Source Topic**

```text
stream_orders
```

**Target Table**

```sql
kafka_data.kafka_bronze.kafka_raw_stream
```

### Silver Layer

Data quality rules applied:

* Remove null Order IDs
* Remove null Amount values
* Remove negative sales amounts
* Convert Unix timestamp to Timestamp
* Remove duplicate orders

**Target Table**

```sql
kafka_data.kafka_silver.clean_orders
```

### Gold Layer

Business aggregations:

* Total Sales by City
* Total Orders by City
* 5-minute window aggregations
* Watermark handling for late-arriving data

**Target Table**

```sql
kafka_data.kafka_gold.aggregated_sales
```

---

## Project Structure

```text
project/
│
├── bronze_ingestion.py
├── silver_transformation.py
├── gold_aggregation.py
├── validation/
│   ├── bronze_validation.py
│   ├── silver_validation.py
│   └── gold_validation.py
│
├── notebooks/
├── screenshots/
└── README.md
```

---

## Streaming Transformations

### Bronze

* Read Kafka Stream
* Parse JSON Messages
* Store Raw Events

### Silver

* Data Cleansing
* Deduplication
* Data Type Conversion
* Quality Validation

### Gold

* Window Aggregation
* Sales Metrics
* City-Level Analytics

---

## Running the Pipeline

### Bronze

```python
query.start()
```

### Silver

```python
silver_query.start()
```

### Gold

```python
gold_query.start()
```

---

## Business Metrics Generated

* Total Sales
* Total Orders
* Sales by City
* Order Volume Trends
* Real-Time Revenue Monitoring

---

## Author

Data Engineering Project demonstrating real time streaming analytics using Kafka, Databricks Structured Streaming, Delta Lake, and Medallion Architecture.
