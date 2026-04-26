# 🏀 WNBA Big Data Pipeline

A historical (1997–2024) WNBA data pipeline built with `pandas` and `nba_api`.
Pulls game logs, player & team stats, draft history, and per-game play-by-play
and advanced box scores from `stats.wnba.com`, cleans and aggregates them, and
generates era-adjusted analytics charts. Runs locally or writes everything to
S3.

## Pipeline stages

| Stage | File | What it does |
|---|---|---|
| 1 — Ingestion | `ingestion.py` | Pulls raw data from `nba_api`: bulk player/team season stats, full schedule, draft history, plus per-game play-by-play and advanced box scores |
| 2 & 3 — Processing | `processing.py` | Cleans logs, parses minutes, computes WIN flag, era-normalizes scoring, builds aggregated frames, writes the two main Parquet outputs |
| 4 — Analytics | `analytics.py` | Builds the qualified-player frame and writes ~12 charts (era-adjusted dominance, all-around composite, efficiency-vs-volume, 3-point trend, distributions, career panels, etc.) |

## Data flow

### 1. Ingestion (`ingestion.py`)

**Source:** `stats.wnba.com` via `nba_api`

For each season 1997–2024 the pipeline pulls:

- Full game schedule (`LeagueGameLog`, player-mode) — every player-game row
- Bulk player season stats (`LeagueDashPlayerStats`)
- Bulk team season stats (`LeagueDashTeamStats`)

It also pulls once for the whole league:

- Draft history (`DraftHistory`)

Then for **every unique `GAME_ID`** in the schedule it fetches and caches:

- Play-by-play (`PlayByPlayV2`)
- Advanced box score (`BoxScoreAdvancedV2`)

**Reliability features** (in `_call_endpoint_safe`):

- Configurable retries with linear backoff (`REQUEST_RETRIES`)
- Per-request timeout (`REQUEST_TIMEOUT`)
- Polite throttling between calls (`REQUEST_PAUSE`)
- File-level cache: per-game `pbp.csv` and `box_adv.csv` are skipped if already on disk, so re-runs are incremental

**Output layout:**
```
data/raw/
├── draft_history.csv
├── game_logs_bulk.csv         # full schedule, all seasons
├── player_stats_bulk.csv      # season-level player stats, all seasons
├── team_stats_bulk.csv        # season-level team stats, all seasons
└── games_cache/
    └── <GAME_ID>/
        ├── pbp.csv            # play-by-play events
        └── box_adv.csv        # advanced box score
```

The `games_cache/` directory is what makes this large — ~6,000 games
× two files each.

### 2 & 3. Processing (`processing.py`)

Both stages live in `processing.run()`. Inputs are the four bulk DataFrames
returned by `ingestion.run()`.

**Cleaning on the game logs:**
- Parse `GAME_DATE` to datetime
- Convert `MIN` to numeric `MINUTES`
- Derive `WIN` (binary from `WL`) and `MONTH` (from date)

**Era normalization:**
- For each season, compute league-wide game-level `PTS` mean and std
- Add a `PPG_ZSCORE` column on the cleaned game logs
- Note: `analytics.py` recomputes z-scores at the season-PPG level for ranking; this column is the per-game flavor

**Aggregations built (returned as a dict from `run()`):**

| Key | Grain | Notes |
|---|---|---|
| `clean_logs` | player-game | Full cleaned game logs with engineered fields |
| `player_stats` | player-season | PPG, GP, mean of `PPG_ZSCORE` |
| `team_stats` | team-season | Adds `OFF_EFF` (PTS / (TOV+1)) and `WIN_PCT` |
| `monthly` | season-month | Average PTS per player-game |
| `clutch` | player (career) | PPG in wins vs losses, `CLUTCH_DELTA` |
| `consistency` | player-season | mean / std of PPG → `CONSISTENCY_SCORE` |

**Persisted to Parquet:**
```
data/processed/
└── clean_logs_massive.parquet           # full cleaned player-game logs

data/aggregated/
└── player_historical_performance.parquet  # player-season summary
```

The other aggregated frames (`team_stats`, `monthly`, `clutch`, `consistency`)
are returned in the dict but **not** written to disk by the current pipeline.
If you want them on disk, add corresponding `helpers.save_parquet` calls in
`processing.run()`.

> **Note on `player_consistency.parquet`:** if a `player_consistency.parquet`
> file already exists in your data dir from an earlier pipeline version, its
> schema (`AVG_PTS`, `STD_PTS`, `consistency`, no `SEASON`) does not match the
> current in-memory `consistency` frame (`avg`, `sd`, `CONSISTENCY_SCORE`,
> with `SEASON`). `analytics.py` handles both, but they are not the same shape.

### 4. Analytics (`analytics.py`)

`analytics.run(processed)` takes the dict from processing and writes charts to
`analytics/output/`.

It first builds a **qualified player-season frame** from `clean_logs` by
aggregating to (player, season) with full box-score columns (PPG, RPG, APG,
SPG, BPG, TOV/G, MPG, 3PA/G, plus-minus, win rate), computing **True Shooting
%**, filtering to `GP ≥ 10`, then z-scoring each stat against the qualified
distribution **within each season** (so era is held constant).

**Charts written:**

```
analytics/output/
├── historical_era_dominance_zscore.png      # top 20 single seasons by era-adjusted PPG
├── historical_dominance_composite.png        # all-around composite (PPG+RPG+APG+SPG+BPG z)
├── efficiency_volume_scatter.png             # PPG vs True Shooting %, colored by season
├── three_point_revolution.png                # league 3PA / player-game and league 3P% over time
├── ppg_distribution_by_season.png            # violin of qualified-player PPG by season
├── league_scoring_trend_historical.png       # mean PTS per player-game by season
├── historical_clutch_leaders.png             # career PPG-in-wins minus PPG-in-losses
├── historical_consistency_ranking.png        # mean/std of PPG, high-volume seasons
└── career_multistat_<Player>.png             # one per player in config.KEY_PLAYERS
                                              #   2x2 panel: PPG, RPG, APG, TS% with z-score overlays
```

## Quickstart

```bash
# Install dependencies
pip install -r requirements.txt

# Run the full pipeline (local storage)
python pipeline.py
```

A full run from scratch will hit the `nba_api` for thousands of games and is
expected to take hours. Subsequent runs reuse the per-game cache and are much
faster.

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
export AWS_SESSION_TOKEN=...          # only for temporary credentials
export AWS_DEFAULT_REGION=us-east-1
```

Then run normally:

```bash
python pipeline.py
```

Files are uploaded to (relative paths mirror local layout):

```
s3://<S3_BUCKET_NAME>/<S3_PREFIX>/data/raw/...
s3://<S3_BUCKET_NAME>/<S3_PREFIX>/data/processed/...
s3://<S3_BUCKET_NAME>/<S3_PREFIX>/data/aggregated/...
s3://<S3_BUCKET_NAME>/<S3_PREFIX>/analytics/output/...
```

S3 uploads are routed through the `save_csv` / `save_parquet` / `save_chart`
helpers in `helpers.py`, so anything written via those helpers is uploaded
automatically. The per-game files in `games_cache/` are written with raw
`pandas.to_csv` and **don't** currently go through the S3 path — they only
land on local disk.

## Configuration

All settings live in **`config.py`**:

| Variable | Default | Description |
|---|---|---|
| `SEASONS` | `['1997', …, '2024']` | Seasons to fetch |
| `TARGET_SEASON` | `'2024'` | Reserved for single-season previews |
| `REQUEST_PAUSE` | `1.0` | Seconds between `nba_api` calls |
| `REQUEST_TIMEOUT` | `60` | Per-call timeout in seconds |
| `REQUEST_RETRIES` | `5` | Retry attempts per endpoint call |
| `BASE_DIR` | `wnba_pipeline_ultra_massive` | Root for local output (env: `WNBA_BASE_DIR`) |
| `KEY_PLAYERS` | A'ja Wilson, Breanna Stewart, Sabrina Ionescu, Caitlin Clark | Players that get a career-trajectory chart |
| `USE_S3` | `false` | Enable S3 uploads (env: `USE_S3`) |
| `S3_BUCKET` | — | Bucket name (env: `S3_BUCKET_NAME`) |
| `S3_PREFIX` | `wnba/pipeline` | Folder prefix inside the bucket (env: `S3_PREFIX`) |
| `S3_ALSO_SAVE_LOCAL` | `true` | Keep local copies when uploading to S3 |

## Use in a notebook

```python
from pipeline import run

run()  # runs all four stages end-to-end and writes outputs

# Or pull individual aggregates after the pipeline ran:
import pandas as pd
player = pd.read_parquet(
    "wnba_pipeline_ultra_massive/data/aggregated/player_historical_performance.parquet"
)
logs = pd.read_parquet(
    "wnba_pipeline_ultra_massive/data/processed/clean_logs_massive.parquet"
)
```

To get the in-memory aggregates that aren't persisted (`team_stats`,
`monthly`, `clutch`, `consistency`), call the stages directly:

```python
import ingestion, processing
raw       = ingestion.run()
processed = processing.run(*raw)
processed['team_stats'].query("SEASON == '2024'")
```

## Project structure

```
wnba_pipeline/
├── config.py        # constants & settings
├── helpers.py       # save utilities (local + S3)
├── ingestion.py     # Stage 1 — nba_api fetching, per-game caching
├── processing.py    # Stages 2 & 3 — cleaning, era-normalization, aggregation
├── analytics.py     # Stage 4 — chart generation
├── pipeline.py      # main runner
├── requirements.txt
└── .gitignore
```