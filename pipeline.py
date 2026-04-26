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

def get_dir_size_gb(directory):
    total_size = 0
    if not os.path.exists(directory): return 0
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            total_size += os.path.getsize(os.path.join(dirpath, f))
    return round(total_size / (1024**3), 2)

def run():
    t0 = time.time()
    print('=' * 65)
    print('  🏀 WNBA ULTRA-MASSIVE PIPELINE (3GB-5GB TARGET)')
    print('=' * 65)

    # Stage 1: Massive Ingestion (Schedule, Bulk, and Game-Level Cache)
    # returns: schedule, player_bulk, team_bulk, draft
    raw_data = ingestion.run()

    # Stage 2 & 3: Processing & Aggregate Generation
    # returns: processed dictionary
    processed = processing.run(*raw_data)

    # Stage 4: Analytics
    analytics.run(processed)

    elapsed = time.time() - t0
    size_gb = get_dir_size_gb(config.RAW_DIR)

    print('\n' + '=' * 65)
    print(f'  Total Data Volume : {size_gb} GB')
    print(f'  Records Processed : {len(processed["clean_logs"]):,}')
    print(f'  Runtime           : {elapsed/3600:.2f} hours')
    print('=' * 65)

if __name__ == '__main__':
    run()