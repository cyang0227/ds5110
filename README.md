# 📊 DS5110 Factor-based Stock Analysis Tool

> A full-stack data science application for quantitative finance, featuring factor analysis, backtesting, and data management.

## 📖 Overview

This project is a comprehensive platform for **Quantitative Factor Investing**. It allows users to:
1.  **Ingest Data**: Fetch financial data (Prices, Fundamentals) and store it in a local warehouse.
2.  **Analyze Stocks**: Visualize price trends and technical indicators for individual securities.
3.  **Backtest Strategies**:
    *   **Technical**: Test single-stock strategies like SMA Crossover, RSI, and MACD.
    *   **Factor**: Test portfolio-level strategies (e.g., "Top 20 Momentum Stocks") with customizable rebalancing, weighting, fees, and slippage.
4.  **Manage Factors**: Calculate and normalize alpha factors (Momentum, Value) using a robust pipeline.

## ✨ Key Features

*   **End-to-End Data Pipeline**: Automated ETL (Extract, Transform, Load) process using **DuckDB**.
*   **High-Performance Backtesting**: Powered by **VectorBT** for lightning-fast vectorised simulations.
*   **Interactive UI**: Built with **Streamlit** and **Plotly** for a responsive and beautiful user experience.
*   **Advanced Factor Logic**:
    *   Z-Score / Rank Standardization
    *   Sector-Neutral vs. Market-Wide Normalization
    *   Custom Transaction Costs (Fees & Slippage)

## 🛠️ Tech Stack

*   **Language**: Python 3.13
*   **Frontend**: Streamlit, Plotly
*   **Database**: DuckDB (Embedded OLAP)
*   **Backtesting**: VectorBT
*   **Data Science**: Pandas, NumPy, Scikit-learn, SciPy

## 🚀 Installation & Setup

### Prerequisites
*   Python 3.13+
*   Conda

### 1. Clone the Repository
```bash
git clone https://github.com/cyang0227/ds5110.git
cd ds5110
```

### 2. Create Environment
This project uses `conda` for dependency management.
```bash
conda env create -f environment_conda.yml
conda activate ds5110
```

## 💡 Usage Guide

### Step 1: Data Ingestion (ETL)
Before running the app, you need to populate the database.
You can do this via the **UI** (Data Management page) or the **Terminal**:
```bash
# Run the full ETL pipeline
python src/etl/run_etl.py
```

### Step 2: Factor Calculation
Calculate factors (Momentum, Value) and store them in the DB.
```bash
python src/pipelines/run_factor_pipeline.py
```

### Step 3: Launch the App
Start the Streamlit web interface:
```bash
streamlit run src/ui/app.py
```
Access the app at `http://localhost:8501`.

### Alternative Database Download
The whole pipeline can be time-consuming (15+ minutes), if you have difficulty running ETL pipeline, you can download the database from [here](https://drive.google.com/file/d/1LmBh74O5lc4wVY_M3c-I8l4fBvY_38Ol/view?usp=drive_link), and place it in the `data/warehouse` directory.

### An AWS EC2 Instance of the app
You can also access the app at [here](http://44.220.64.89:8501/).

## 📂 Project Structure

```text
src/
├── backtest/       # VectorBT backtesting engine
│   └── engine.py
├── etl/            # Data ingestion and transformation
│   ├── fetch_*.py
│   └── load_*.py
├── factors/        # Factor definitions and calculations
│   ├── momentum/
│   └── value/
├── pipelines/      # Orchestration scripts
│   └── run_factor_pipeline.py
├── ui/             # Streamlit frontend application
│   └── app.py
└── utils/          # Helper functions (DB, Paths, Data)
```
