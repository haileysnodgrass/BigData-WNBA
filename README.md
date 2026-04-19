# 🏀 WNBA Big Data Pipeline

A multi-season WNBA data pipeline built with Pandas and `nba_api`.  
Pulls game logs, player stats, and rosters from `stats.wnba.com`, cleans and aggregates the data, and generates analytics charts — all with optional AWS S3 output.

## Pipeline stages

| Stage | File | What it does |
|---|---|---|
| 1 — Ingestion | `ingestion.py` | Pulls raw data from `nba_api` (game logs, leaders, player stats, rosters) |
| 2 — Storage | `processing.py` | Writes raw data to Parquet |
| 3 — Processing | `processing.py` | Cleans, engineers features, builds aggregated datasets |
| 4 — Analytics | `analytics.py` | Generates and saves 10+ charts |

## Data Flow Walkthrough
## 1. Data Ingestion (Input Layer)

**Source:** `stats.wnba.com` via `nba_api`

The pipeline pulls:

- Player-level game logs (every game)
- League leaders
- Player season statistics
- Team rosters

**Key features:**
- Automatic retries + timeout handling
- Request throttling to avoid API blocking
- Caching layer (reuses previously saved data)

**Output:**
```
data/raw/
├── game_logs_2018_2024.csv
├── league_leaders_2018_2024.csv
├── player_season_stats_raw_2018_2024.csv
└── rosters_2018_2024.csv
```

## 2. Storage Layer (Raw → Parquet)

Raw CSV data is converted into **Parquet format** for efficient storage and analytics.

**Why Parquet:**
- Columnar storage (faster queries)
- Better compression (smaller size)
- Standard format in big data systems

**Output:**
```
data/processed/
├── game_logs_2018_2024.parquet
├── league_leaders_2018_2024.parquet
├── player_season_stats_2018_2024.parquet
└── rosters_2018_2024.parquet
```

## 3. Data Processing & Feature Engineering

Raw data is cleaned and transformed into structured datasets.

### Cleaning
- Parse dates
- Convert minutes to numeric
- Create derived fields:
  - `MONTH`
  - `WIN` (binary outcome)

### Aggregations

#### Player-level (per season)
- PPG, RPG, APG, etc.
- Win percentage

#### Team-level
- Scoring, shooting %, turnovers
- Offensive efficiency (`OFF_EFF`)

#### Time-based
- Monthly scoring trends

#### Performance
- Top single-game performances

**Output:**
```
data/aggregated/
├── player_stats_by_season.parquet
├── team_stats_by_season.parquet
├── monthly_scoring.parquet
└── top_performances.parquet
```

## 4. Advanced Analytics

The pipeline computes deeper analytical insights:

### What predicts winning
- Correlation between team metrics and win percentage
- Identifies key drivers of team success

### Clutch scoring proxy
- Compares player scoring in wins vs losses
- Measures performance differences based on outcomes

### Player consistency
- Evaluates scoring stability using mean vs variability

**Output:**
```
data/aggregated/
├── win_predictors.parquet
├── clutch_scoring_proxy.parquet
└── player_consistency.parquet
```

## 5. Analytics & Visualization (Output Layer)

The pipeline generates charts for both descriptive and analytical insights.

### Core charts
- Top scorers
- Team win % vs scoring
- Offensive efficiency
- Monthly scoring heatmap
- Player scoring trends

### Analytical charts
- What predicts winning
- Clutch scoring leaders
- Most consistent scorers

**Output:**
```
analytics/output/
├── top_scorers_2024.png
├── team_win_vs_scoring_2024.png
├── team_efficiency_2024.png
├── monthly_scoring_heatmap_2024.png
├── top_game_performances_2018_2024.png
├── ppg_trend_by_season_2018_2024.png
├── win_predictors.png
├── clutch_scoring_proxy.png
└── player_consistency.png
```

## Quickstart

```bash
# Install dependencies
pip install -r requirements.txt

# Run the full pipeline (local storage)
python pipeline.py
```

## AWS S3 output

Set these environment variables before running to write all outputs to S3:

```bash
export USE_S3=true
export S3_BUCKET_NAME=my-wnba-bucket
export S3_PREFIX=wnba/pipeline        # optional, default: wnba/pipeline
export S3_ALSO_SAVE_LOCAL=true        # keep local copies too (default: true)

# Credentials (or use an IAM role / ~/.aws/config)
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...          # required for temporary credentials
export AWS_DEFAULT_REGION=us-east-1
```

Then run normally:

```bash
python pipeline.py
```

Files are uploaded to:
```
s3://<S3_BUCKET_NAME>/<S3_PREFIX>/data/raw/
s3://<S3_BUCKET_NAME>/<S3_PREFIX>/data/processed/
s3://<S3_BUCKET_NAME>/<S3_PREFIX>/data/aggregated/
s3://<S3_BUCKET_NAME>/<S3_PREFIX>/analytics/output/
```

## Configuration

All settings live in **`config.py`**:

| Variable | Default | Description |
|---|---|---|
| `SEASONS` | `['2018'…'2024']` | Seasons to fetch |
| `TARGET_SEASON` | `'2024'` | Season used for single-season charts |
| `REQUEST_PAUSE` | `1.5` | Seconds between API calls |
| `BASE_DIR` | `wnba_pipeline_data` | Root for local output |
| `USE_S3` | `false` | Enable S3 uploads |
| `S3_BUCKET` | — | Bucket name (env: `S3_BUCKET_NAME`) |

## Use in a notebook

```python
from pipeline import run

processed = run()

# Access any aggregated dataset
processed['player_stats'].head()
processed['team_stats'].query("SEASON == '2024'")
```

## Project structure

```
wnba_pipeline/
├── config.py        # all constants & settings
├── helpers.py       # save utilities (local + S3)
├── ingestion.py     # Stage 1 — nba_api fetching
├── processing.py    # Stages 2 & 3 — Parquet + cleaning + aggregation
├── analytics.py     # Stage 4 — chart generation
├── pipeline.py      # main runner
├── requirements.txt
└── .gitignore
```
