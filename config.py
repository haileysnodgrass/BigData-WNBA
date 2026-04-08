# ── config.py ─────────────────────────────────────────────────────────────────
# Central configuration for the WNBA pipeline.
# Edit this file to change seasons, paths, or S3 settings.
# ──────────────────────────────────────────────────────────────────────────────

import os

# ── Season settings ────────────────────────────────────────────────────────────
SEASONS       = ['2018', '2019', '2020', '2021', '2022', '2023', '2024']
TARGET_SEASON = '2024'        # used for single-season charts & previews

# ── API settings ───────────────────────────────────────────────────────────────
WNBA_LEAGUE_ID = '10'
REQUEST_PAUSE  = 1.5          # seconds between nba_api calls (be polite)

# ── Local directory layout ─────────────────────────────────────────────────────
BASE_DIR      = os.getenv('WNBA_BASE_DIR', 'wnba_pipeline_data')
RAW_DIR       = os.path.join(BASE_DIR, 'data', 'raw')
PROCESSED_DIR = os.path.join(BASE_DIR, 'data', 'processed')
AGG_DIR       = os.path.join(BASE_DIR, 'data', 'aggregated')
OUTPUT_DIR    = os.path.join(BASE_DIR, 'analytics', 'output')

LOCAL_DIRS = [RAW_DIR, PROCESSED_DIR, AGG_DIR, OUTPUT_DIR]

# ── AWS S3 settings ────────────────────────────────────────────────────────────
# Set USE_S3=True to write all outputs to S3 instead of (or in addition to)
# local disk.  Credentials are read from environment variables or ~/.aws/config.
#
# Required env vars when USE_S3=True:
#   AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION  (or IAM role)
#   S3_BUCKET_NAME  — the bucket to write to, e.g. "my-wnba-data"
#
# Optional:
#   S3_PREFIX       — folder prefix inside the bucket, e.g. "wnba/pipeline"
#   USE_S3          — "true" / "false"  (default false)
#   S3_ALSO_SAVE_LOCAL — "true" / "false"  keep local copies too (default true)

USE_S3             = os.getenv('USE_S3', 'false').lower() == 'true'
S3_BUCKET          = os.getenv('S3_BUCKET_NAME', '')
S3_PREFIX          = os.getenv('S3_PREFIX', 'wnba/pipeline').rstrip('/')
S3_ALSO_SAVE_LOCAL = os.getenv('S3_ALSO_SAVE_LOCAL', 'true').lower() == 'true'

# ── Chart styling ──────────────────────────────────────────────────────────────
WNBA_ORANGE = '#F47321'
WNBA_TEAL   = '#009999'
WNBA_PURPLE = '#7B2FBE'

# ── WNBA team registry ─────────────────────────────────────────────────────────
WNBA_TEAMS = {
    1611661322: 'New York Liberty',
    1611661329: 'Las Vegas Aces',
    1611661325: 'Seattle Storm',
    1611661320: 'Chicago Sky',
    1611661319: 'Atlanta Dream',
    1611661323: 'Phoenix Mercury',
    1611661326: 'Minnesota Lynx',
    1611661328: 'Connecticut Sun',
    1611661324: 'Indiana Fever',
    1611661321: 'Dallas Wings',
    1611661327: 'Los Angeles Sparks',
    1611661330: 'Washington Mystics',
}

# ── Key players tracked in multi-season trend charts ──────────────────────────
KEY_PLAYERS = ["A'ja Wilson", 'Breanna Stewart', 'Sabrina Ionescu', 'Caitlin Clark']
