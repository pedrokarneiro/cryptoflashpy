# Cryptoflashpy
**A Data Engineering Pipeline for the CryptoFlash Project**

This project implements an automated ETL (Extract, Transform, Load) pipeline responsible for data ingestion and pre-processing for the CryptoFlash project. It was developed as part of the **Large Scale and Multi-Structured Databases** discipline at the **University of Pisa**, A.Y. 2025/2026.

## 👥 Team & Academic Context
* **Students:** Chandrakant Yadav, Pedro Carneiro Junior
* **Institution:** Università di Pisa
* **Course:** Master’s in AI and Data Engineering
* **Professor:** Prof. Pietro Ducange

---

## 🏗 System Architecture
The project is containerized using **Docker** to ensure a reproducible environment across different host systems (WSL, Linux, or macOS).

* **Ingestion Layer:** Python-based Kaggle API integration for automated dataset retrieval.
* **Processing Layer:** Pandas-based cleaning, temporal normalization, and memory-efficient chunking.
* **Storage Layer:** * **Raw:** Local filesystem volume mapping for immutable CSV storage.
    * **Processed:** **MongoDB (NoSQL)** for high-performance, multi-structured document storage for dynamic querying.
* **Orchestration:** A central master script (`run_pipeline.py`) manages a "Fail-Fast" task flow with integrated environment sanity checks.

---

## 📂 Project Structure
```text
cryptoflashpy/
├── data/
│   ├── raw/                # Original datasets & EDA reports (Immutable)
│   └── processed/          # Final EDA reports (Database source verification)
├── src/
│   ├── get_bitcoin_data.py    # Ingestion: BTC via Kaggle
│   ├── get_ethereum_data.py   # Ingestion: ETH via Kaggle
│   ├── eda.py                 # Data Audit: Generates Markdown reports (Before/After)
│   ├── process_bitcoin.py     # Transformation & Mongo Injection: BTC
│   ├── process_ethereum.py    # Transformation & Mongo Injection: ETH
│   ├── run_pipeline.py        # Master Orchestrator (with Log Generation)
│   ├── check_env.py           # Sanity Check: Validates Mongo & Python deps
│   └── connection_test.py     # Connectivity Diagnostic
├── .env                    # Secrets & API Keys
├── docker-compose.yml      # Environment Definition (Mongo + Python)
└── requirements.txt        # Python Dependencies (Pandas, PyMongo, TQDM)
```

---

## 🚀 Quick Start

### 1. Prerequisites
* Docker & Docker Compose
* Kaggle API Credentials (Username and Key)

### 2. Environment Configuration
Ensure your `.env` file or `docker-compose.yml` environment block contains your Kaggle credentials and the MongoDB connection string:
`MONGO_URI=mongodb://mongodb:27017/`

### 3. Container Initialization
Build and start the infrastructure:
```bash
docker compose up -d
docker exec -it cryptoflashpy-dev bash
```

### 4. Execute the Pipeline
Inside the container, run the orchestrator. It will perform a sanity check, download the data, generate audits, and populate MongoDB:

```
python run_pipeline.py
```

---

## 📋 Monitoring & Logs
The pipeline generates real-time feedback and persistent logs:

* **`pipeline_execution.log`:** Detailed timestamps and duration for every task.

* **Progress Bars:** Memory-efficient `tqdm` bars for the 7.4M+ Bitcoin and Ethereum rows injection.

---

## 📊 Data Lineage & Audit Trail

### Bitcoin (BTC)
* **Source:** `mczielinski/bitcoin-historical-data`
* **Frequency:** 1-Minute Intervals (~7.4M records)
* **Processing:** Unix Epoch to Datetime conversion, removal of trade-gap nulls, and memory-efficient chunked injection.
* **Audit:** Automated EDA report generated before and after MongoDB insertion.

### Ethereum (ETH)
* **Source:** `viniciusqroz/ethereum-historical-data`
* **Frequency:** Daily Intervals
* **Processing:** Schema standardization (Date string to Timestamp), feature reordering, and field mapping for NoSQL compatibility.
* **Audit:** Post-injection verification via MongoDB query dashboard.

---

## 🛠 Tech Stack
* **Language:** Python 3.12
* **Database:** MongoDB (Running in a separate Docker service)
* **Data Handling:** Pandas, PyMongo
* **Infrastructure:** Docker, Docker Compose, WSL
* **Monitoring:** TQDM (Progress tracking), Python `logging` module

---

## 🏛 Project Purpose
This layer serves as the foundation for the **CryptoFlash** engine. By transforming raw, multi-structured CSV data into a high-performance NoSQL document store, we enable the distributed Java trading components to perform complex historical analysis and simulate high-frequency trading scenarios with minimal latency.