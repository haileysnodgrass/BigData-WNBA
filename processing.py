# ── processing.py ─────────────────────────────────────────────────────────────
# Stage 2 — Raw → Parquet storage
# Stage 3 — Cleaning, feature engineering & aggregation
# ──────────────────────────────────────────────────────────────────────────────

import os
import pandas as pd
import numpy as np
import logging
import config
import helpers

def _parse_minutes(min_series):
    def _convert(val):
        try:
            if isinstance(val, (int, float)): return float(val)
            parts = str(val).split(':')
            return int(parts[0]) + int(parts[1]) / 60
        except: return 0.0
    return min_series.apply(_convert)

def run(df_schedule, df_ps_bulk, df_ts_bulk, df_draft):
    logging.info('Stage 2/3 — Processing Massive Data & Solving Era-Normalization...')
    
    # 1. Clean Game Logs
    df_clean = df_schedule.copy()
    df_clean['GAME_DATE'] = pd.to_datetime(df_clean['GAME_DATE'], errors='coerce')
    df_clean['MINUTES'] = _parse_minutes(df_clean['MIN'])
    df_clean['WIN'] = df_clean['WL'].str.upper().eq('W').astype(int)
    df_clean['MONTH'] = df_clean['GAME_DATE'].dt.month
    
    # 2. CALCULATE ERA NORMALIZATION (Z-SCORE) ON RAW LOGS
    # We do this before aggregating so we have the standard deviation of all games in a season
    season_stats = df_clean.groupby('SEASON')['PTS'].agg(['mean', 'std']).reset_index()
    df_clean = df_clean.merge(season_stats, on='SEASON', how='left')
    df_clean['PPG_ZSCORE'] = (df_clean['PTS'] - df_clean['mean']) / (df_clean['std'] + 1e-9)
    df_clean = df_clean.drop(columns=['mean', 'std'])

    # 3. BUILD AGGREGATED DATASETS
    # Player Stats (FIX: Now includes PPG_ZSCORE for Analytics)
    player_stats = df_clean.groupby(['PLAYER_NAME', 'PLAYER_ID', 'SEASON']).agg(
        PPG=('PTS', 'mean'), 
        GP=('GAME_ID', 'nunique'),
        PPG_ZSCORE=('PPG_ZSCORE', 'mean')
    ).reset_index().round(3)
    
    # Team Stats
    team_stats = df_ts_bulk.copy()
    team_stats['OFF_EFF'] = (team_stats['PTS'] / (team_stats['TOV'] + 1)).round(2)
    team_stats['WIN_PCT'] = team_stats['W'] / (team_stats['W'] + team_stats['L'])
    
    # Monthly
    monthly = df_clean.groupby(['SEASON', 'MONTH'])['PTS'].mean().reset_index().rename(columns={'PTS': 'AVG_PTS'})
    
    # Clutch Scoring
    clutch = df_clean.groupby(['PLAYER_NAME', 'WIN'])['PTS'].mean().unstack().fillna(0).reset_index()
    clutch = clutch.rename(columns={0: 'PTS_LOSS', 1: 'PTS_WIN'})
    clutch['CLUTCH_DELTA'] = (clutch['PTS_WIN'] - clutch['PTS_LOSS']).round(3)
    
    # Player Consistency
    player_consistency = df_clean.groupby(['PLAYER_NAME', 'SEASON']).agg(
        avg=('PTS', 'mean'),
        sd=('PTS', 'std')
    ).reset_index()
    player_consistency['CONSISTENCY_SCORE'] = (player_consistency['avg'] / (player_consistency['sd'] + 1e-5)).round(3)
    
    # Save processed files to Parquet
    helpers.save_parquet(df_clean, config.PROCESSED_DIR, "clean_logs_massive.parquet")
    helpers.save_parquet(player_stats, config.AGG_DIR, "player_historical_performance.parquet")

    # 4. Return Dictionary with keys
    return {
        'clean_logs': df_clean,
        'player_stats': player_stats,
        'team_stats': team_stats,
        'monthly': monthly,
        'clutch': clutch,                 
        'consistency': player_consistency
    }