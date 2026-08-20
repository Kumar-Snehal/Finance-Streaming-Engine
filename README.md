# Real-Time Financial Streaming & Visualization Pipeline

A containerized real-time data streaming and analytics pipeline built to process high-frequency market ticks. This project demonstrates modern event-driven architecture using **Python**, **Redpanda (Kafka-compatible)**, **Apache Flink**, **TimescaleDB**, and **Grafana**, orchestrated seamlessly via **Docker Compose** and custom Bash automation.

## Architecture & Data Flow

```
[Python Producer] 
       │ (Simulates Market Ticks)
       ▼
[Redpanda (Kafka Broker)] 
       │ (Event Streaming)
       ▼
[Apache Flink (Java Engine)] 
       │ (10-second Tumbling Window VWAP Computation)
       ▼
[TimescaleDB (PostgreSQL)] 
       │ (Time-Series Hypertable Storage)
       ▼
[Grafana Dashboard] 
       (Real-Time Multi-Ticker Visualization)
```

1. **Producer (`market_producer.py`):** A multi-threaded Python application that continuously generates synthetic market ticks (price, volume, and timestamps) for multiple stock tickers (`AAPL`, `GOOGL`, `MSFT`, `AMZN`, `NVDA`, `TSLA`).
2. **Broker (`Redpanda`):** A lightweight, high-performance Kafka-API compatible event broker that ingests and buffers the live tick stream.
3. **Stream Processor (`Apache Flink`):** A Java-based real-time analytics engine that consumes raw ticks from Redpanda, computes the **Volume-Weighted Average Price (VWAP)** over 10-second tumbling windows, and structures the output.
4. **Storage (`TimescaleDB`):** A specialized time-series PostgreSQL database optimized for fast writes and high-performance range queries, housing the processed hypertable (`stock_vwap`).
5. **Visualization (`Grafana`):** An interactive observability platform rendering live, color-separated trend lines for each asset class.

---

## Tech Stack

* **Language/Frameworks:** Python (Multithreading, Kafka Client), Java / Apache Flink (Maven), SQL.
* **Streaming & Brokers:** Redpanda (Kafka API).
* **Database:** TimescaleDB (PostgreSQL Time-Series).
* **Visualization:** Grafana.
* **Orchestration & Automation:** Docker Compose, Linux Bash Scripting.

---

## Quick Start / Setup

### Prerequisites

* Linux environment (Ubuntu / Fedora / Arch)
* Docker & Docker Compose
* Python 3.x & Maven installed locally

### 1. Clone & Configure Environment

Navigate to your project root and set up your Python virtual environment for the producer:

```bash
python3 -m venv stream_env
source stream_env/bin/activate
pip install kafka-python
```

### 2. Run the Automated Pipeline

An orchestration script (`start_pipeline.sh`) is provided to spin up Docker containers, verify readiness, launch the multi-threaded Python producer, boot the Flink stream processor, and stream logs:

```bash
chmod +x start_pipeline.sh
./start_pipeline.sh
```
*(Pressing `Ctrl+C` will gracefully terminate background tasks and tear down the Docker container stack).*
---

## Accessing Web Consoles & UIs

Once the pipeline is running, you can access the service endpoints:
* **Grafana Dashboard:** `http://localhost:3000` *(Default login: admin / password)*
* **Redpanda Console (Kafka UI):** `http://localhost:8080`
* **TimescaleDB / Postgres:** `docker compose exec timescaledb psql -U admin -d finance`
---

## Dashboard Configuration

To display distinct, color-coded lines for each individual stock ticker in Grafana:

1. Create a **Time series** panel using your PostgreSQL data source.
2. Use the following SQL query format with table partitioning:
```sql
SELECT 
    time, 
    ticker, 
    vwap 
FROM stock_vwap 
WHERE $__timeFilter(time) 
ORDER BY time ASC;
```

3. Apply the **Partition by values** transformation in Grafana keyed on the `ticker` column.