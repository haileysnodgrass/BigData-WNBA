# ── processing.py ─────────────────────────────────────────────────────────────
# Stage 2 — Raw → Parquet storage
# Stage 3 — Cleaning, feature engineering & aggregation
# ──────────────────────────────────────────────────────────────────────────────

import pandas as pd
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
import config
import helpers


# ── Stage 2: persist raw data as Parquet ──────────────────────────────────────

def store_parquet(df_game_logs, df_leaders, df_player_season_stats, df_rosters):
    """Write raw DataFrames to Parquet for efficient downstream reads."""
    logging.info('\nStage 2 — Writing Parquet files...')
    helpers.save_parquet(df_game_logs,          config.PROCESSED_DIR, 'game_logs_2018_2024.parquet',          'Game logs (raw)')
    helpers.save_parquet(df_leaders,            config.PROCESSED_DIR, 'league_leaders_2018_2024.parquet',     'League leaders (raw)')
    helpers.save_parquet(df_player_season_stats,config.PROCESSED_DIR, 'player_season_stats_2018_2024.parquet','Player season stats (raw)')
    helpers.save_parquet(df_rosters,            config.PROCESSED_DIR, 'rosters_2018_2024.parquet',            'Rosters (raw)')
    logging.info('Stage 2 complete.\n')


# ── Stage 3: clean & aggregate ────────────────────────────────────────────────

def _parse_minutes(min_series: pd.Series) -> pd.Series:
    """Convert minutes (already numeric or 'MM:SS' strings) to float."""
    def _convert(val):
        try:
            # If it's already a number, return it
            if isinstance(val, (int, float)):
                return float(val)
            # Otherwise parse 'MM:SS' format
            parts = str(val).split(':')
            return int(parts[0]) + int(parts[1]) / 60
        except Exception:
            return None
    return min_series.apply(_convert)


def clean_game_logs(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the raw game-log DataFrame.

    - Parses GAME_DATE to datetime
    - Converts MIN to float minutes
    - Derives MONTH and WIN columns
    - Drops all *_RANK columns (useful only for API filtering)
    """
    df = df_raw.copy()
    df = helpers.standardize_season(df)

    df['GAME_DATE'] = pd.to_datetime(df['GAME_DATE'], errors='coerce')
    df['MONTH']     = df['GAME_DATE'].dt.month
    df['MINUTES']   = _parse_minutes(df['MIN'])
    df['WIN']       = df['WL'].str.upper().eq('W').astype(int)

    rank_cols = [c for c in df.columns if c.endswith('_RANK')]
    df = df.drop(columns=rank_cols)

    helpers.require_rows(df, 'Cleaned game logs')
    return df


def build_player_stats(df_clean: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate per-player, per-season stats (PPG, RPG, APG, etc.).
    """
    agg = (
        df_clean
        .groupby(['PLAYER_NAME', 'PLAYER_ID', 'SEASON'], as_index=False)
        .agg(
            GP    = ('GAME_ID', 'nunique'),
            PPG   = ('PTS',  'mean'),
            RPG   = ('REB',  'mean'),
            APG   = ('AST',  'mean'),
            SPG   = ('STL',  'mean'),
            BPG   = ('BLK',  'mean'),
            TPG   = ('TOV',  'mean'),
            MPG   = ('MINUTES', 'mean'),
            WIN_PCT=('WIN', 'mean'),
        )
    )
    for col in ['PPG','RPG','APG','SPG','BPG','TPG','MPG','WIN_PCT']:
        agg[col] = agg[col].round(2)

    helpers.require_rows(agg, 'Player stats aggregated')
    helpers.save_parquet(agg, config.AGG_DIR, 'player_stats_by_season.parquet', 'Player stats by season')
    return agg


def build_team_stats(df_clean: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate per-team, per-season stats including offensive efficiency.
    """
    agg = (
        df_clean
        .groupby(['TEAM_ABBREVIATION', 'SEASON'], as_index=False)
        .agg(
            GP          = ('GAME_ID', 'nunique'),
            WIN_PCT     = ('WIN', 'mean'),
            PPG         = ('PTS', 'mean'),
            RPG         = ('REB', 'mean'),
            APG         = ('AST', 'mean'),
            TPG         = ('TOV', 'mean'),
            FG_PCT      = ('FG_PCT', 'mean'),
            FG3_PCT     = ('FG3_PCT', 'mean'),
            FT_PCT      = ('FT_PCT', 'mean'),
        )
    )
    # Simple offensive-efficiency proxy: PPG adjusted for turnovers
    agg['OFF_EFF'] = (agg['PPG'] / (agg['TPG'] + 1)).round(2)

    for col in ['WIN_PCT','PPG','RPG','APG','TPG','FG_PCT','FG3_PCT','FT_PCT']:
        agg[col] = agg[col].round(3)

    helpers.require_rows(agg, 'Team stats aggregated')
    helpers.save_parquet(agg, config.AGG_DIR, 'team_stats_by_season.parquet', 'Team stats by season')
    return agg


def build_monthly_scoring(df_clean: pd.DataFrame) -> pd.DataFrame:
    """Average points per game grouped by season + month."""
    agg = (
        df_clean
        .groupby(['SEASON', 'MONTH'], as_index=False)
        ['PTS'].mean()
        .rename(columns={'PTS': 'AVG_PTS'})
    )
    agg['AVG_PTS'] = agg['AVG_PTS'].round(2)
    helpers.save_parquet(agg, config.AGG_DIR, 'monthly_scoring.parquet', 'Monthly scoring')
    return agg


def build_top_performances(df_clean: pd.DataFrame, top_n: int = 50) -> pd.DataFrame:
    """
    Top single-game scoring performances across all seasons.
    """
    cols = ['PLAYER_NAME', 'TEAM_ABBREVIATION', 'SEASON', 'GAME_DATE', 'MATCHUP', 'PTS', 'REB', 'AST']
    agg = df_clean[cols].nlargest(top_n, 'PTS').reset_index(drop=True)
    helpers.save_parquet(agg, config.AGG_DIR, 'top_performances.parquet', f'Top {top_n} game performances')
    return agg

def analyze_what_predicts_winning(df_team_stats: pd.DataFrame) -> pd.DataFrame:
    """
    Compute correlation of team metrics with WIN_PCT.
    """
    features = ['PPG', 'RPG', 'APG', 'TPG', 'FG_PCT', 'OFF_EFF', 'WIN_PCT']
    df = df_team_stats[features].copy()

    corr = df.corr(numeric_only=True)
    win_corr = (
        corr[['WIN_PCT']]
        .sort_values('WIN_PCT', ascending=False)
        .reset_index()
        .rename(columns={'index': 'METRIC', 'WIN_PCT': 'CORR_WITH_WIN_PCT'})
    )

    helpers.save_parquet(
        win_corr,
        config.AGG_DIR,
        'win_predictors.parquet',
        'Win predictors'
    )
    return win_corr


def analyze_clutch_scoring(df_clean: pd.DataFrame, min_games: int = 10) -> pd.DataFrame:
    """
    Proxy for clutch scoring: compare a player's average points in wins vs losses.
    """
    player_games = (
        df_clean.groupby('PLAYER_NAME')['GAME_ID']
        .nunique()
        .reset_index(name='GP')
    )

    grouped = (
        df_clean.groupby(['PLAYER_NAME', 'WIN'])['PTS']
        .mean()
        .unstack()
        .rename(columns={0: 'PTS_in_Loss', 1: 'PTS_in_Win'})
        .reset_index()
    )

    result = grouped.merge(player_games, on='PLAYER_NAME', how='left')
    result = result[result['GP'] >= min_games].copy()

    result['PTS_in_Loss'] = result['PTS_in_Loss'].fillna(0)
    result['PTS_in_Win'] = result['PTS_in_Win'].fillna(0)
    result['Clutch_Diff'] = result['PTS_in_Win'] - result['PTS_in_Loss']

    result = result.sort_values('Clutch_Diff', ascending=False).reset_index(drop=True)

    helpers.save_parquet(
        result,
        config.AGG_DIR,
        'clutch_scoring_proxy.parquet',
        'Clutch scoring proxy'
    )
    return result


def analyze_player_consistency(df_clean: pd.DataFrame, min_games: int = 10) -> pd.DataFrame:
    """
    Measure player scoring consistency using mean points divided by standard deviation.
    Higher values = more consistent scoring relative to output.
    """
    stats = (
        df_clean.groupby('PLAYER_NAME')
        .agg(
            GP=('GAME_ID', 'nunique'),
            AVG_PTS=('PTS', 'mean'),
            STD_PTS=('PTS', 'std'),
        )
        .reset_index()
    )

    stats = stats[stats['GP'] >= min_games].copy()
    stats['STD_PTS'] = stats['STD_PTS'].fillna(0)
    stats['consistency'] = stats['AVG_PTS'] / (stats['STD_PTS'] + 1e-5)
    stats = stats.sort_values('consistency', ascending=False).reset_index(drop=True)

    helpers.save_parquet(
        stats,
        config.AGG_DIR,
        'player_consistency.parquet',
        'Player consistency'
    )
    return stats

def run(df_game_logs, df_leaders, df_player_season_stats, df_rosters):
    """
    Run Stages 2 & 3.  Returns a dict of processed DataFrames.
    """
    store_parquet(df_game_logs, df_leaders, df_player_season_stats, df_rosters)

    logging.info('Stage 3 — Cleaning & aggregating...')
    df_clean        = clean_game_logs(df_game_logs)
    df_player_stats = build_player_stats(df_clean)
    df_team_stats   = build_team_stats(df_clean)
    df_monthly      = build_monthly_scoring(df_clean)
    df_top_perf     = build_top_performances(df_clean)
    win_predictors     = analyze_what_predicts_winning(df_team_stats)
    clutch_scoring     = analyze_clutch_scoring(df_clean)
    player_consistency = analyze_player_consistency(df_clean)
    logging.info('Stage 3 complete.\n')

    return {
        'clean_logs':    df_clean,
        'player_stats':  df_player_stats,
        'team_stats':    df_team_stats,
        'monthly':       df_monthly,
        'top_perf':      df_top_perf,
        'win_predictors':      win_predictors,
        'clutch_scoring':      clutch_scoring,
        'player_consistency':  player_consistency,
    }
