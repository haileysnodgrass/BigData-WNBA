# ── ingestion.py ──────────────────────────────────────────────────────────────
# Stage 1 — Data Ingestion
# Pulls WNBA game logs, league leaders, player season stats, and rosters
# from stats.wnba.com via the nba_api library. No API key required.
# ──────────────────────────────────────────────────────────────────────────────

import time
import requests
import pandas as pd
from nba_api.stats.endpoints import (
    LeagueGameLog,
    LeagueLeaders,
    LeagueDashPlayerStats,
)
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
import config
import helpers

def _call_endpoint(endpoint_cls, **kwargs):
    last_err = None
    for attempt in range(1, config.REQUEST_RETRIES + 1):
        try:
            endpoint = endpoint_cls(
                headers=config.NBA_HEADERS,
                timeout=config.REQUEST_TIMEOUT,
                **kwargs,
            )
            return endpoint.get_data_frames()[0]
        except requests.exceptions.RequestException as e:
            last_err = e
            wait = min(5 * attempt, 20)
            logging.warning(f"  ⚠️ Request failed (attempt {attempt}/{config.REQUEST_RETRIES}): {e}")
            if attempt < config.REQUEST_RETRIES:
                logging.info(f"  ↻ Retrying in {wait}s...")
                time.sleep(wait)
    raise last_err

def fetch_game_logs_and_leaders(seasons=None, pause=None):
    """
    Fetch per-game player logs and league leaders for each season.

    Returns
    -------
    df_game_logs : pd.DataFrame  — combined game logs across all seasons
    df_leaders   : pd.DataFrame  — combined league leaders across all seasons
    """
    seasons = seasons or config.SEASONS
    pause   = pause   or config.REQUEST_PAUSE

    all_game_logs = []
    all_leaders   = []

    for season in seasons:
        logging.info(f'\nFetching season {season}...')

        # Game logs
        gl = _call_endpoint(
            LeagueGameLog,
            season=season,
            league_id=config.WNBA_LEAGUE_ID,
            player_or_team_abbreviation='P',
        )
        gl['SEASON'] = season
        all_game_logs.append(gl)
        logging.info(f'  Game logs   — {len(gl):,} rows')

        time.sleep(pause)

        # League leaders
        ll = _call_endpoint(
            LeagueLeaders,
            season=season,
            league_id=config.WNBA_LEAGUE_ID,
        )
        ll['SEASON'] = season
        all_leaders.append(ll)
        logging.info(f'  Leaders     — {len(ll):,} rows')

        time.sleep(pause)

    df_game_logs = pd.concat(all_game_logs, ignore_index=True)
    df_leaders   = pd.concat(all_leaders,   ignore_index=True)

    df_game_logs = helpers.standardize_season(df_game_logs)
    df_leaders   = helpers.standardize_season(df_leaders)

    helpers.save_csv(df_game_logs, config.RAW_DIR, 'game_logs_2018_2024.csv',    'Game logs (all seasons)')
    helpers.save_csv(df_leaders,   config.RAW_DIR, 'league_leaders_2018_2024.csv', 'Leaders (all seasons)')

    logging.info(f'\nTotal game log rows: {len(df_game_logs):,}')
    return df_game_logs, df_leaders


def fetch_player_season_stats(seasons=None, pause=None):
    """
    Fetch aggregated per-season player stats and build a roster table.

    Returns
    -------
    df_stats   : pd.DataFrame  — player season stats (all seasons)
    df_rosters : pd.DataFrame  — slim roster (player + team + season)
    """
    seasons = seasons or config.SEASONS
    pause   = pause   or config.REQUEST_PAUSE

    all_stats = []

    logging.info('\nFetching player season stats (all seasons)...')
    for season in seasons:
        logging.info(f'\n📅 Season {season}...')
        stats = _call_endpoint(
            LeagueDashPlayerStats,
            season=season,
            league_id_nullable=config.WNBA_LEAGUE_ID,
        )
        stats['SEASON'] = season
        all_stats.append(stats)
        logging.info(f'  {len(stats):,} player-season rows')
        time.sleep(pause)

    df_stats = pd.concat(all_stats, ignore_index=True)
    df_stats = helpers.standardize_season(df_stats)

    helpers.save_csv(
        df_stats, config.RAW_DIR,
        'player_season_stats_raw_2018_2024.csv',
        'Player season stats (all seasons)',
    )

    # Build slim roster table
    roster_cols = ['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'SEASON']
    df_rosters = df_stats[roster_cols].drop_duplicates().reset_index(drop=True)

    for abbr, grp in df_rosters.groupby('TEAM_ABBREVIATION'):
        logging.info(f'  {abbr:<5} {len(grp):,} player-season records')

    helpers.save_csv(df_rosters, config.RAW_DIR, 'rosters_2018_2024.csv', 'All rosters combined')

    logging.info(f'\n Stage 1 complete — Unique player-season records: {len(df_rosters):,}')
    return df_stats, df_rosters


def run(seasons=None):
    """Run the full ingestion stage. Returns all four raw DataFrames."""
    helpers.ensure_local_dirs()
    df_game_logs, df_leaders            = fetch_game_logs_and_leaders(seasons)
    df_player_season_stats, df_rosters  = fetch_player_season_stats(seasons)
    return df_game_logs, df_leaders, df_player_season_stats, df_rosters


if __name__ == '__main__':
    run()
