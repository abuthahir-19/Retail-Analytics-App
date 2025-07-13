# Real-Time Retail Analytics Platform

## Overview
The Real-Time Retail Analytics Platform is an end-to-end data engineering project designed to process and analyze streaming retail data in real time. The pipeline ingests data from multiple sources (e.g., orders, payments, products), applies a variety of transformations to derive actionable insights, and stores the results in ClickHouse for high-performance analytics. The project supports use cases such as sales monitoring, fraud detection, customer segmentation, and logistics optimization, enabling data-driven decision-making for retail businesses.
This repository contains the implementation of a scalable Spark ETL pipeline written in Java, integrated with Apache Kafka for data ingestion and ClickHouse for storage. The project includes unit tests for robust validation and is deployed on a cloud environment (e.g., AWS).

![Architecture Diagram](resources/images/architecture.svg)
## Project Objectives
- **Ingest real-time data** from retail datasets (orders, order items, payments, products, customers, sellers) using Kafka.
- **Apply diverse transformations** including data cleaning, enrichment, aggregations, anomaly detection, and feature engineering to generate insights.
- **Store transformed data in ClickHouse** for low-latency analytical queries.
- **Ensure scalability and fault tolerance** with Spark Structured Streaming and checkpointing.
- **Validate pipeline logic** through comprehensive unit tests using JUnit and spark-testing-base.

## Tech Stack
- **Data Processing:** Apache Spark (Structured Streaming, Java API)
- **Data Ingestion:** Apache Kafka
- **Data Storage:** ClickHouse (MergeTree engine)
- **Raw Data Store:** Apache Hadoop
- **Programming Language:** Java
- **Testing:** JUnit, spark-testing-base
- **Deployment:** Docker, AWS (or other cloud platforms)
- **Monitoring (Planned):** Prometheus, ELK Stack
- **Visualization (Future):** Grafana, Power BI

## Architecture
The pipeline follows a modular architecture for real-time data processing:
1. **Data Ingestion:**
    - Streams data from Kafka topics (orders, order_items, order_payments, products, sellers).
    - Topics will get data published from a static dataset by this way implementing a simulated environment of streaming data.
    - Uses **ConsumeDataFromKafka** utilizer to read data with predefined schemas.
2. **Data Transformation:**
    - **Cleaning:** Filters invalid records (e.g., canceled orders, duplicates).
    - **Enrichment:** Joins datasets to add customer and seller demographics.
    - **Aggregation:** Computes metrics like total payment value by payment type, top 10 transactions, and average delivery delays.
    - **Anomaly Detection:** Flags high-value transactions and rapid repeat purchases.
    - **Feature Engineering:** Derives metrics like customer lifetime value and product co-purchase patterns.
3. **Data Storage:**
    - Writes transformed datasets to ClickHouse tables (total_payment_by_type, top_transactions, overall_top_transactions, etc.) using the clickhouse-jdbc connector.
    - Uses MergeTree engine for optimized analytical queries.
4. **Future Enhancements:**
    - Integrate Grafana or Power BI for real-time dashboards to visualize KPIs (e.g., sales trends, fraud alerts).
    - Implement monitoring with Prometheus and ELK Stack for pipeline health.

## Key Transformations
The pipeline implements a wide range of transformations to support retail analytics:
- **Data Cleaning:** Removed canceled orders and duplicates for accurate metrics.
- **Enrichment:** Added customer and seller location details for regional analysis.
- **Aggregation:** Calculated total sales by payment type, top product categories, and average order value.
- **Anomaly Detection:** Flagged high-value transactions and rapid repeat purchases for fraud detection.
- **Feature Engineering:** Computed delivery delays, customer lifetime value, and product co-purchase patterns.
- **Schema Evolution:** Handled missing fields (e.g., product dimensions) with default values.

## Setup Instructions
### Prerequisites
- Java 11 or higher
- Apache Spark 3.5.3
- Apache Hadoop 3.3.6
- Apache Kafka 3.x
- ClickHouse 24.x
- Maven for dependency management
- Docker (optional, for containerized deployment)
- AWS account (optional, for cloud deployment)

# Installation Guide

Run the following commands to set up the project:

1. **Clone the repository**
```bash
git clone https://github.com/your-username/real-time-retail-analytics.git
cd real-time-retail-analytics
```
2. **Install Dependencies:**
```bash
mvn clean install
```
3. **Configure Kafka:**
   - Set up Kafka brokers and create topics (orders, order_items, order_payments, products, sellers).
   - Update Constants.java with your Kafka bootstrap servers and topic names.
4. **Configure ClickHouse Connection:**
   - Update FlagHighValueOrders.java with your ClickHouse JDBC URL, user, and password.

## Running the Pipeline
1. **Build the Project:**
```bash
mvn package
```
2. **Run the Spark Application:**
```bash
spark-submit --class com.learnJava.Main --master local[*] --name SparkCheck --master local[*] --driver-memory 2g target/Retail_Analytics_Platform-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs://192.168.1.37:9000/user/abuthahir/retail_project/static_resource/clickhouse_config.properties
```