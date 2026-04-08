# ── pipeline.py ───────────────────────────────────────────────────────────────
# Main entry point — runs all four pipeline stages end-to-end.
#
# Usage
# -----
#   python pipeline.py                         # local storage
#   USE_S3=true S3_BUCKET_NAME=my-bucket \
#     python pipeline.py                       # upload to S3
#
# Or import and call run() from a notebook:
#   from pipeline import run
#   run()
# ──────────────────────────────────────────────────────────────────────────────

import os
import time

import config
import helpers
import ingestion
import processing
import analytics


def run(seasons=None):
    t0 = time.time()

    print('=' * 55)
    print('  🏀 WNBA Big Data Pipeline')
    print(f'  Seasons      : {config.SEASONS}')
    print(f'  Target season: {config.TARGET_SEASON}')
    print(f'  Storage      : {"S3 → s3://" + config.S3_BUCKET if config.USE_S3 else "Local → " + config.BASE_DIR}')
    print('=' * 55)

    # Stage 1 — Ingestion
    df_game_logs, df_leaders, df_player_stats, df_rosters = ingestion.run(seasons)

    # Stages 2 & 3 — Storage + Processing
    processed = processing.run(df_game_logs, df_leaders, df_player_stats, df_rosters)

    # Stage 4 — Analytics
    analytics.run(processed)

    elapsed = time.time() - t0
    raw_files   = len(os.listdir(config.RAW_DIR))       if os.path.exists(config.RAW_DIR)       else '—'
    proc_files  = len(os.listdir(config.PROCESSED_DIR)) if os.path.exists(config.PROCESSED_DIR) else '—'
    agg_files   = len(os.listdir(config.AGG_DIR))       if os.path.exists(config.AGG_DIR)       else '—'
    chart_files = len(os.listdir(config.OUTPUT_DIR))    if os.path.exists(config.OUTPUT_DIR)    else '—'

    print('=' * 55)
    print('  🏀 Pipeline Complete!')
    print('=' * 55)
    print(f'  Stage 1 — Ingestion   : {raw_files:>2} raw CSV files')
    print(f'  Stage 2 — Storage     : {proc_files:>2} Parquet files')
    print(f'  Stage 3 — Processing  : {agg_files:>2} aggregated datasets')
    print(f'  Stage 4 — Analytics   : {chart_files:>2} charts generated')
    print(f'  Total rows            : {len(processed["clean_logs"]):,}')
    print(f'  Wall time             : {elapsed:.0f}s')
    print('=' * 55)

    return processed


if __name__ == '__main__':
    run()
