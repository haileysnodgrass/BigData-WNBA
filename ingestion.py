# ── ingestion.py ──────────────────────────────────────────────────────────────
# Stage 1 — Data Ingestion
# Pulls WNBA game logs, league leaders, player season stats, and rosters
# from stats.wnba.com via the nba_api library.
# ──────────────────────────────────────────────────────────────────────────────

import os
import time
import logging
import pandas as pd
from nba_api.stats.endpoints import (
    LeagueGameLog,
    PlayByPlayV2,
    BoxScoreAdvancedV2,
    DraftHistory,
    PlayerAwards,
    LeagueDashPlayerStats,
    LeagueDashTeamStats
)
import config
import helpers

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')

def _call_endpoint_safe(endpoint_cls, label, **kwargs):
    for attempt in range(1, config.REQUEST_RETRIES + 1):
        try:
            endpoint = endpoint_cls(headers=config.NBA_HEADERS, timeout=config.REQUEST_TIMEOUT, **kwargs)
            dfs = endpoint.get_data_frames()
            return dfs[0] if dfs else pd.DataFrame()
        except Exception as e:
            if "resultSet" in str(e): return pd.DataFrame()
            time.sleep(attempt * 2)
    return pd.DataFrame()

def fetch_massive_game_data():
    """Fetches PBP, Shots, and Adv Box Scores for all historical games."""
    helpers.ensure_local_dirs()
    
    # 1. Get Schedule & Bulk Stats
    logging.info("Step 1: Building Game Inventory & Bulk Stats...")
    all_games = []
    all_player_stats = []
    all_team_stats = []
    
    # Draft History
    df_draft = _call_endpoint_safe(DraftHistory, "Draft History", league_id=config.WNBA_LEAGUE_ID)
    helpers.save_csv(df_draft, config.RAW_DIR, "draft_history.csv", "Draft History")

    for season in config.SEASONS:
        # Schedule
        gl = _call_endpoint_safe(LeagueGameLog, f"Schedule {season}", season=season, league_id=config.WNBA_LEAGUE_ID, player_or_team_abbreviation='P')
        if not gl.empty: 
            gl['SEASON'] = season
            all_games.append(gl)
        
        # Seasonal Player Stats
        ps = _call_endpoint_safe(LeagueDashPlayerStats, f"Player Stats {season}", season=season, league_id_nullable=config.WNBA_LEAGUE_ID)
        if not ps.empty:
            ps['SEASON'] = season
            all_player_stats.append(ps)
            
        # Seasonal Team Stats
        ts = _call_endpoint_safe(LeagueDashTeamStats, f"Team Stats {season}", season=season, league_id_nullable=config.WNBA_LEAGUE_ID)
        if not ts.empty:
            ts['SEASON'] = season
            all_team_stats.append(ts)
            
        time.sleep(config.REQUEST_PAUSE)

    df_schedule = pd.concat(all_games)
    df_ps_bulk = pd.concat(all_player_stats)
    df_ts_bulk = pd.concat(all_team_stats)
    
    helpers.save_csv(df_schedule, config.RAW_DIR, "game_logs_bulk.csv", "Full Schedule")
    helpers.save_csv(df_ps_bulk, config.RAW_DIR, "player_stats_bulk.csv", "Seasonal Player Stats")
    helpers.save_csv(df_ts_bulk, config.RAW_DIR, "team_stats_bulk.csv", "Seasonal Team Stats")

    # 2. Iterate Game IDs (PBP, Shots, Adv Box)
    game_ids = df_schedule['GAME_ID'].unique()
    logging.info(f"Targeting {len(game_ids)} games for Event-Level Ingestion...")

    for count, gid in enumerate(game_ids, 1):
        game_folder = os.path.join(config.GAMES_CACHE, gid)
        os.makedirs(game_folder, exist_ok=True)
        
        # Files
        pbp_file = os.path.join(game_folder, 'pbp.csv')
        # shot_file = os.path.join(game_folder, 'shots.csv')
        adv_file = os.path.join(game_folder, 'box_adv.csv')

        if not os.path.exists(pbp_file):
            pbp = _call_endpoint_safe(PlayByPlayV2, f"PBP {gid}", game_id=gid)
            if not pbp.empty: pbp.to_csv(pbp_file, index=False)
            time.sleep(config.REQUEST_PAUSE)

        # if not os.path.exists(shot_file):
        #     shots = _call_endpoint_safe(ShotChartDetail, f"Shots {gid}", game_id=gid, league_id=config.WNBA_LEAGUE_ID, team_id=0, player_id=0, context_measure_simple='FGA')
        #     if not shots.empty: shots.to_csv(shot_file, index=False)
        #     time.sleep(config.REQUEST_PAUSE)

        if not os.path.exists(adv_file):
            adv = _call_endpoint_safe(BoxScoreAdvancedV2, f"Adv Box {gid}", game_id=gid)
            if not adv.empty: adv.to_csv(adv_file, index=False)
            time.sleep(config.REQUEST_PAUSE)

        if count % 100 == 0:
            logging.info(f"Scraped {count}/{len(game_ids)} games...")

    return df_schedule, df_ps_bulk, df_ts_bulk, df_draft

def run(seasons=None):
    return fetch_massive_game_data()