# Cryptoflashpy
**A Robust Data Engineering Pipeline for Cryptocurrency Analysis**

This project implements an automated ETL (Extract, Transform, Load) pipeline that fetches historical cryptocurrency data from Kaggle, processes it into a standardized format, and stores it in high-performance Parquet files.

## 🏗 System Architecture
The project is containerized using **Docker** and runs within a **WSL (Windows Subsystem for Linux)** environment.

* **Ingestion Layer:** Python-based Kaggle API integration.
* **Processing Layer:** Pandas-based cleaning and temporal normalization.
* **Storage Layer:** Local filesystem volume mapping, utilizing Apache Parquet for optimized I/O.
* **Orchestration:** A central master script manages task dependencies and execution flow.

---

## 📂 Project Structure
\`\`\`text
cryptoflashpy/
├── data/
│   ├── raw/                # Original datasets (Immutable)
│   └── processed/          # Transformed Parquet files
├── src/
│   ├── get_bitcoin_data.py    # Ingestion: BTC
│   ├── get_ethereum_data.py   # Ingestion: ETH
│   ├── process_bitcoin.py     # Transformation: BTC
│   ├── process_ethereum.py    # Transformation: ETH
│   ├── run_pipeline.py        # Master Orchestrator
│   └── connection_test.py     # Connectivity Diagnostic
├── .env                    # Secrets & API Keys
├── docker-compose.yml      # Environment Definition
└── requirements.txt        # Python Dependencies
\`\`\`

---

## 🚀 Quick Start

### 1. Prerequisites
* Docker & Docker Compose
* Kaggle API Credentials

### 2. Environment Configuration
Ensure your \`docker-compose.yml\` environment block is updated with your Kaggle credentials.

### 3. Container Initialization
Build and start the development environment:
\`\`\`bash
docker compose up -d
docker exec -it cryptoflashpy-dev bash
\`\`\`

### 4. Execute the Pipeline
Inside the container, run the orchestrator to fetch and process all data:
\`\`\`bash
python run_pipeline.py
\`\`\`

---

## 📊 Data Lineage

### Bitcoin (BTC)
* **Source:** \`mczielinski/bitcoin-historical-data\`
* **Frequency:** 1-Minute
* **Processing:** Unix Epoch to Datetime conversion, removal of trade-gap nulls.

### Ethereum (ETH)
* **Source:** \`viniciusqroz/ethereum-historical-data\`
* **Frequency:** Daily
* **Processing:** Schema standardization (Date string to Timestamp), feature reordering.

---

## 🛠 Tech Stack
* **Language:** Python 3.12
* **Data Handling:** Pandas, PyArrow
* **Infrastructure:** Docker, WSL
* **API:** Kaggle API
