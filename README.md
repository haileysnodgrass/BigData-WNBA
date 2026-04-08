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
