# # ── analytics.py ──────────────────────────────────────────────────────────────
# # Stage 4 — Era-Normalized Historical Analytics (1997–2024)
# # ──────────────────────────────────────────────────────────────────────────────

# import warnings
# import matplotlib.pyplot as plt
# import matplotlib.ticker as mtick
# import seaborn as sns
# import logging
# import pandas as pd
# import config
# import helpers

# logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
# warnings.filterwarnings('ignore')
# sns.set_theme(style='darkgrid', palette='muted')
# plt.rcParams['figure.dpi'] = 130


# def chart_era_adjusted_dominance(df_player_stats):
#     """Top historical seasons based on Era-Normalized Z-Score."""
#     z_col = 'PPG_ZSCORE' 
#     if z_col not in df_player_stats.columns:
#         logging.error(f"❌ Missing {z_col} in player stats. Skipping dominance chart.")
#         return

#     top = df_player_stats.nlargest(20, z_col)
#     fig, ax = plt.subplots(figsize=(12, 8))
#     labels = [f"{row['PLAYER_NAME']} ({row['SEASON']})" for _, row in top.iterrows()]
    
#     bars = ax.barh(labels, top[z_col], color=config.WNBA_ORANGE, edgecolor='white')
#     ax.bar_label(bars, fmt='%.2f', padding=4, fontsize=9)
    
#     ax.set_xlabel('PPG Z-Score (Std Devs above Era Mean)')
#     ax.set_title('Most Dominant Scoring Seasons — WNBA History (1997–2024)', fontsize=14, fontweight='bold')
#     ax.invert_yaxis()
#     plt.tight_layout()
#     helpers.save_chart(fig, config.OUTPUT_DIR, 'historical_era_dominance_zscore.png')
#     plt.close(fig)


# def chart_league_scoring_trend(df_player_stats):
#     """Visualizes how league scoring has evolved since 1997."""
#     trend = df_player_stats.groupby('SEASON')['PPG'].mean().reset_index()
#     trend['SEASON_INT'] = trend['SEASON'].astype(int)
#     trend = trend.sort_values('SEASON_INT')
    
#     fig, ax = plt.subplots(figsize=(12, 5))
#     ax.plot(trend['SEASON'], trend['PPG'], marker='o', color=config.WNBA_TEAL, linewidth=2.5)
#     ax.fill_between(trend['SEASON'], trend['PPG'], alpha=0.1, color=config.WNBA_TEAL)
    
#     ax.set_xticks(trend['SEASON'][::2])
#     ax.set_ylabel('Avg Points Per Game')
#     ax.set_title('League-Wide Scoring Trends (1997–2024)', fontsize=14, fontweight='bold')
#     plt.tight_layout()
#     helpers.save_chart(fig, config.OUTPUT_DIR, 'league_scoring_trend_historical.png')
#     plt.close(fig)


# def chart_clutch_leaders(df_clutch):
#     """Top 'Clutch' players (PTS in Wins vs PTS in Losses)."""
#     if 'CLUTCH_DELTA' not in df_clutch.columns: return
#     top = df_clutch.sort_values('CLUTCH_DELTA', ascending=False).head(15)
    
#     fig, ax = plt.subplots(figsize=(11, 6))
#     bars = ax.barh(top['PLAYER_NAME'], top['CLUTCH_DELTA'], color=config.WNBA_TEAL)
#     ax.bar_label(bars, fmt='%.2f', padding=4, fontsize=9)
    
#     ax.set_xlabel('Point Delta (Wins - Losses)')
#     ax.set_title('Top Clutch Scoring Proxy Leaders (Career)', fontsize=14, fontweight='bold')
#     ax.invert_yaxis()
#     plt.tight_layout()
#     helpers.save_chart(fig, config.OUTPUT_DIR, 'historical_clutch_leaders.png')
#     plt.close(fig)


# def chart_consistency_ranking(df_consistency):
#     """Shows the most consistent scorers (Highest Mean/SD Ratio)."""
#     if 'CONSISTENCY_SCORE' not in df_consistency.columns: return
#     top = df_consistency[df_consistency['avg'] > 10].nlargest(15, 'CONSISTENCY_SCORE')
    
#     fig, ax = plt.subplots(figsize=(11, 6))
#     labels = [f"{row['PLAYER_NAME']} ({row['SEASON']})" for _, row in top.iterrows()]
#     bars = ax.barh(labels, top['CONSISTENCY_SCORE'], color=config.WNBA_ORANGE)
#     ax.bar_label(bars, fmt='%.2f', padding=4, fontsize=9)
    
#     ax.set_xlabel('Consistency Score (Mean / Std Dev)')
#     ax.set_title('Most Consistent High-Volume Scoring Seasons', fontsize=14, fontweight='bold')
#     ax.invert_yaxis()
#     plt.tight_layout()
#     helpers.save_chart(fig, config.OUTPUT_DIR, 'historical_consistency_ranking.png')
#     plt.close(fig)


# def chart_player_career_trend(player_name, df_player_stats):
#     """Plots career trajectory showing raw PPG vs Era-Adjusted Z-Score."""
#     pdf = df_player_stats[df_player_stats['PLAYER_NAME'] == player_name].copy()
#     if pdf.empty: return
    
#     pdf['SEASON_INT'] = pdf['SEASON'].astype(int)
#     pdf = pdf.sort_values('SEASON_INT')
    
#     fig, ax1 = plt.subplots(figsize=(10, 5))
#     ax1.bar(pdf['SEASON'], pdf['PPG'], color=config.WNBA_TEAL, alpha=0.3, label='PPG')
#     ax1.set_ylabel('Points Per Game', color=config.WNBA_TEAL)
    
#     ax2 = ax1.twinx()
#     ax2.plot(pdf['SEASON'], pdf['PPG_ZSCORE'], marker='o', color=config.WNBA_ORANGE, linewidth=2, label='Z-Score')
#     ax2.set_ylabel('Z-Score (Era Adjusted)', color=config.WNBA_ORANGE)
    
#     plt.title(f'Career Trajectory: {player_name}', fontsize=14, fontweight='bold')
#     plt.tight_layout()
    
#     safe_name = player_name.replace("'", "").replace(" ", "_")
#     helpers.save_chart(fig, config.OUTPUT_DIR, f'career_trend_{safe_name}.png')
#     plt.close(fig)


# def run(processed):
#     """Run Stage 4 Analytics."""
#     logging.info('Stage 4 — Generating historical visualizations...')

#     df_player = processed['player_stats']
#     df_cons   = processed['consistency']
#     df_clutch = processed['clutch']
    
#     chart_era_adjusted_dominance(df_player)
#     chart_league_scoring_trend(df_player)
#     chart_clutch_leaders(df_clutch)
#     chart_consistency_ranking(df_cons)

#     for player in config.KEY_PLAYERS:
#         chart_player_career_trend(player, df_player)

#     logging.info(f'Stage 4 complete — Historical charts saved to {config.OUTPUT_DIR}\n')

# ── analytics.py ──────────────────────────────────────────────────────────────
# Stage 4 — Era-Normalized Historical Analytics (1997–2024)
#
# Improvements over the prior version:
#   1. Builds a richer per-(player, season) frame from clean_logs that uses
#      the full box-score (REB/AST/STL/BLK/TOV/FGA/FTA/3PT/MIN/+/-).
#   2. Era-adjusts via season-PPG distribution across QUALIFIED players
#      (GP ≥ MIN_GAMES_QUALIFIED), instead of averaging game-level z-scores.
#   3. Adds True Shooting %, three-point trend, scoring distribution by season,
#      and an all-around dominance composite.
#   4. Player career chart is now a multi-stat panel (PPG, RPG, APG, TS%).
#   5. Defensive against the saved-parquet schema for player_consistency
#      (which differs from what processing.py returns at runtime).
# ──────────────────────────────────────────────────────────────────────────────

import warnings
import logging
from typing import Optional

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns

import config
import helpers

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
warnings.filterwarnings('ignore')
sns.set_theme(style='darkgrid', palette='muted')
plt.rcParams['figure.dpi'] = 130

MIN_GAMES_QUALIFIED = 10           # for season-leader & era-adjusted rankings
MIN_GAMES_CAREER    = 100          # for career-level rankings (e.g. clutch)


# ── feature builder ──────────────────────────────────────────────────────────

def build_qualified_player_seasons(clean_logs: pd.DataFrame,
                                   min_games: int = MIN_GAMES_QUALIFIED
                                   ) -> pd.DataFrame:
    """Aggregate clean_logs to per-(PLAYER, SEASON) with full stat lines and
    properly era-normalized z-scores (across qualified players, per season).

    Returns the qualified frame; an unfiltered version isn't returned because
    nothing downstream needs it.
    """
    grp = clean_logs.groupby(['PLAYER_NAME', 'PLAYER_ID', 'SEASON'])
    agg = grp.agg(
        GP        = ('GAME_ID',     'nunique'),
        PPG       = ('PTS',         'mean'),
        RPG       = ('REB',         'mean'),
        APG       = ('AST',         'mean'),
        SPG       = ('STL',         'mean'),
        BPG       = ('BLK',         'mean'),
        TOV_PG    = ('TOV',         'mean'),
        MPG       = ('MINUTES',     'mean'),
        FG3A_PG   = ('FG3A',        'mean'),
        FG3M_PG   = ('FG3M',        'mean'),
        TOTAL_PTS = ('PTS',         'sum'),
        TOTAL_FGA = ('FGA',         'sum'),
        TOTAL_FTA = ('FTA',         'sum'),
        TOTAL_FG3A= ('FG3A',        'sum'),
        TOTAL_FG3M= ('FG3M',        'sum'),
        PM_AVG    = ('PLUS_MINUS',  'mean'),   # NaN for pre-1998
        WIN_RATE  = ('WIN',         'mean'),
    ).reset_index()

    # Efficiency metrics
    ts_denom = 2 * (agg['TOTAL_FGA'] + 0.44 * agg['TOTAL_FTA'])
    agg['TS_PCT']  = np.where(ts_denom > 0, agg['TOTAL_PTS'] / ts_denom, np.nan)
    agg['FG3_PCT'] = np.where(agg['TOTAL_FG3A'] > 0,
                              agg['TOTAL_FG3M'] / agg['TOTAL_FG3A'], np.nan)

    qualified = agg[agg['GP'] >= min_games].copy()

    # Proper era-adjusted z-scores: distribution of qualified players, per season
    for stat in ['PPG', 'RPG', 'APG', 'SPG', 'BPG', 'TS_PCT']:
        season_grp = qualified.groupby('SEASON')[stat]
        means = season_grp.transform('mean')
        sds   = season_grp.transform('std').replace(0, np.nan)
        qualified[f'{stat}_Z'] = (qualified[stat] - means) / sds

    # All-around composite — sum of positive era-adjusted contributions
    z_cols = ['PPG_Z', 'RPG_Z', 'APG_Z', 'SPG_Z', 'BPG_Z']
    qualified['DOMINANCE_Z'] = qualified[z_cols].fillna(0).clip(lower=0).sum(axis=1) \
                              + qualified[z_cols].fillna(0).clip(upper=0).sum(axis=1)
    # (the above keeps signs honest: penalize below-avg, reward above-avg)

    logging.info(f'  qualified player-seasons: {len(qualified):,} '
                 f'(filter: GP ≥ {min_games})')
    return qualified


# ── charts ───────────────────────────────────────────────────────────────────

def chart_era_adjusted_dominance(qualified: pd.DataFrame, top_n: int = 20):
    """Top scoring seasons, z-scored against qualified players that season."""
    top = qualified.nlargest(top_n, 'PPG_Z')
    fig, ax = plt.subplots(figsize=(12, 8))
    labels = [f"{r['PLAYER_NAME']} ({r['SEASON']}) — {r['PPG']:.1f} PPG"
              for _, r in top.iterrows()]
    bars = ax.barh(labels, top['PPG_Z'], color=config.WNBA_ORANGE, edgecolor='white')
    ax.bar_label(bars, fmt='%.2f', padding=4, fontsize=9)
    ax.set_xlabel('PPG Z-Score (vs qualified players that season)')
    ax.set_title('Most Dominant Scoring Seasons — Era-Adjusted (1997–2024)',
                 fontsize=14, fontweight='bold')
    ax.invert_yaxis()
    plt.tight_layout()
    helpers.save_chart(fig, config.OUTPUT_DIR, 'historical_era_dominance_zscore.png')
    plt.close(fig)


def chart_dominance_composite(qualified: pd.DataFrame, top_n: int = 20):
    """Stacked z-score contributions: most well-rounded single seasons."""
    top = qualified.nlargest(top_n, 'DOMINANCE_Z')
    fig, ax = plt.subplots(figsize=(12, 8))
    labels = [f"{r['PLAYER_NAME']} ({r['SEASON']})" for _, r in top.iterrows()]

    cats   = [('PPG_Z', 'Scoring'), ('RPG_Z', 'Rebounding'),
              ('APG_Z', 'Playmaking'), ('SPG_Z', 'Steals'), ('BPG_Z', 'Blocks')]
    colors = ['#F47321', '#009999', '#7B2FBE', '#4CAF50', '#E91E63']

    left = np.zeros(len(top))
    for (col, lbl), color in zip(cats, colors):
        vals = top[col].fillna(0).clip(lower=0).values   # only show positive contribution
        ax.barh(labels, vals, left=left, color=color, label=lbl, edgecolor='white')
        left += vals

    ax.set_xlabel('Sum of positive z-score contributions')
    ax.set_title('Most All-Around Dominant Seasons (PPG · RPG · APG · SPG · BPG)',
                 fontsize=14, fontweight='bold')
    ax.invert_yaxis()
    ax.legend(loc='lower right', fontsize=9, frameon=True)
    plt.tight_layout()
    helpers.save_chart(fig, config.OUTPUT_DIR, 'historical_dominance_composite.png')
    plt.close(fig)


def chart_efficiency_volume(qualified: pd.DataFrame):
    """Volume vs efficiency: PPG against True Shooting %, colored by era."""
    df = qualified.dropna(subset=['TS_PCT']).copy()
    df = df[df['GP'] >= 15]
    df['SEASON_INT'] = df['SEASON'].astype(int)

    fig, ax = plt.subplots(figsize=(12, 7))
    sc = ax.scatter(df['PPG'], df['TS_PCT'], c=df['SEASON_INT'],
                    cmap='viridis', alpha=0.55, s=22, edgecolor='none')
    cbar = plt.colorbar(sc, ax=ax)
    cbar.set_label('Season')

    # Annotate elite seasons (high volume + above-median efficiency).
    # To avoid overlap we keep at most one label per integer-PPG bin.
    elite = df[(df['PPG'] >= 20) & (df['TS_PCT'] >= 0.55)].copy()
    elite['ppg_bin'] = elite['PPG'].round().astype(int)
    elite = (elite.sort_values('TS_PCT', ascending=False)
                  .drop_duplicates('ppg_bin')
                  .sort_values('PPG', ascending=False)
                  .head(8))
    for i, (_, r) in enumerate(elite.iterrows()):
        # alternate offset above/below to reduce remaining collisions
        dy = 12 if i % 2 == 0 else -14
        ax.annotate(f"{r['PLAYER_NAME']} '{str(r['SEASON'])[-2:]}",
                    (r['PPG'], r['TS_PCT']),
                    fontsize=8, alpha=0.95,
                    xytext=(6, dy), textcoords='offset points',
                    arrowprops=dict(arrowstyle='-', color='gray',
                                    alpha=0.5, linewidth=0.6))

    median_ts = df['TS_PCT'].median()
    median_pp = df['PPG'].median()
    ax.axhline(median_ts, linestyle='--', linewidth=0.7, color='gray', alpha=0.6)
    ax.axvline(median_pp, linestyle='--', linewidth=0.7, color='gray', alpha=0.6)

    ax.set_xlabel('Points Per Game')
    ax.set_ylabel('True Shooting %  =  PTS / (2 · (FGA + 0.44 · FTA))')
    ax.set_title('Volume vs Efficiency — All Qualified Player-Seasons',
                 fontsize=14, fontweight='bold')
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    plt.tight_layout()
    helpers.save_chart(fig, config.OUTPUT_DIR, 'efficiency_volume_scatter.png')
    plt.close(fig)


def chart_three_point_revolution(clean_logs: pd.DataFrame):
    """League-wide 3-point attempts per player-game and league 3P%, by season."""
    by_season = clean_logs.groupby('SEASON').agg(
        FG3A_per_game=('FG3A', 'mean'),
        FG3M_per_game=('FG3M', 'mean'),
    ).reset_index()
    by_season['FG3_PCT'] = np.where(by_season['FG3A_per_game'] > 0,
                                    by_season['FG3M_per_game'] / by_season['FG3A_per_game'],
                                    np.nan)
    by_season['SEASON_INT'] = by_season['SEASON'].astype(int)
    by_season = by_season.sort_values('SEASON_INT')

    fig, ax1 = plt.subplots(figsize=(12, 5))
    ax1.bar(by_season['SEASON'], by_season['FG3A_per_game'],
            color=config.WNBA_ORANGE, alpha=0.75, label='3PA / player-game')
    ax1.set_ylabel('3-Point Attempts per player-game', color=config.WNBA_ORANGE)
    ax1.tick_params(axis='x', rotation=45)

    ax2 = ax1.twinx()
    ax2.plot(by_season['SEASON'], by_season['FG3_PCT'],
             marker='o', color=config.WNBA_TEAL, linewidth=2.2, label='League 3P%')
    ax2.set_ylabel('League 3-Point %', color=config.WNBA_TEAL)
    ax2.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))

    plt.title('The 3-Point Revolution in the WNBA',
              fontsize=14, fontweight='bold')
    plt.tight_layout()
    helpers.save_chart(fig, config.OUTPUT_DIR, 'three_point_revolution.png')
    plt.close(fig)


def chart_ppg_distribution_by_season(qualified: pd.DataFrame):
    """Violin plot: shape of qualified-player scoring distribution per season."""
    df = qualified.copy()
    df['SEASON_INT'] = df['SEASON'].astype(int)
    df = df.sort_values('SEASON_INT')
    seasons = sorted(df['SEASON_INT'].unique())
    data = [df[df['SEASON_INT'] == s]['PPG'].values for s in seasons]

    fig, ax = plt.subplots(figsize=(13, 6))
    parts = ax.violinplot(data, positions=seasons, showmedians=True, widths=0.85)
    for pc in parts['bodies']:
        pc.set_facecolor(config.WNBA_TEAL)
        pc.set_edgecolor('white')
        pc.set_alpha(0.55)
    for key in ('cmedians', 'cmins', 'cmaxes', 'cbars'):
        if key in parts:
            parts[key].set_color('#333')
            parts[key].set_linewidth(0.8)

    ax.set_xlabel('Season')
    ax.set_ylabel(f'PPG  (qualified players, GP ≥ {MIN_GAMES_QUALIFIED})')
    ax.set_title('Distribution of Qualified-Player Scoring by Season',
                 fontsize=14, fontweight='bold')
    ax.set_xticks(seasons[::2])
    plt.tight_layout()
    helpers.save_chart(fig, config.OUTPUT_DIR, 'ppg_distribution_by_season.png')
    plt.close(fig)


def chart_clutch_leaders(df_clutch: pd.DataFrame):
    """Top clutch-proxy players by point delta in Wins vs Losses (career)."""
    if 'CLUTCH_DELTA' not in df_clutch.columns:
        return
    top = df_clutch.sort_values('CLUTCH_DELTA', ascending=False).head(15)
    fig, ax = plt.subplots(figsize=(11, 6))
    bars = ax.barh(top['PLAYER_NAME'], top['CLUTCH_DELTA'], color=config.WNBA_TEAL)
    ax.bar_label(bars, fmt='%.2f', padding=4, fontsize=9)
    ax.set_xlabel('Point Delta  (PPG in wins  −  PPG in losses)')
    ax.set_title('Clutch Scoring Proxy Leaders (Career)',
                 fontsize=14, fontweight='bold')
    ax.invert_yaxis()
    plt.tight_layout()
    helpers.save_chart(fig, config.OUTPUT_DIR, 'historical_clutch_leaders.png')
    plt.close(fig)


def chart_consistency_ranking(df_consistency: pd.DataFrame):
    """Most consistent high-volume scoring seasons.

    Defensive: the saved player_consistency.parquet uses different column names
    than processing.run() returns at runtime. Handle both.
    """
    df = df_consistency.copy()
    rename = {'AVG_PTS': 'avg', 'STD_PTS': 'sd', 'consistency': 'CONSISTENCY_SCORE'}
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    needed = {'PLAYER_NAME', 'avg', 'CONSISTENCY_SCORE'}
    if not needed.issubset(df.columns):
        logging.warning(f'consistency frame missing {needed - set(df.columns)} — skipping')
        return

    df = df[df['avg'] > 10]
    if df.empty:
        return
    top = df.nlargest(15, 'CONSISTENCY_SCORE')

    label_col = 'SEASON' if 'SEASON' in top.columns else None
    if label_col:
        labels = [f"{r['PLAYER_NAME']} ({r[label_col]})" for _, r in top.iterrows()]
    else:
        labels = top['PLAYER_NAME'].tolist()

    fig, ax = plt.subplots(figsize=(11, 6))
    bars = ax.barh(labels, top['CONSISTENCY_SCORE'], color=config.WNBA_ORANGE)
    ax.bar_label(bars, fmt='%.2f', padding=4, fontsize=9)
    ax.set_xlabel('Consistency Score (mean / std-dev of PPG)')
    title = ('Most Consistent High-Volume Scorers'
             if not label_col else 'Most Consistent High-Volume Scoring Seasons')
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.invert_yaxis()
    plt.tight_layout()
    helpers.save_chart(fig, config.OUTPUT_DIR, 'historical_consistency_ranking.png')
    plt.close(fig)


def chart_player_multistat_career(player_name: str, qualified: pd.DataFrame):
    """Career trajectory across four panels: PPG, RPG, APG, TS% — each with z-score line."""
    pdf = qualified[qualified['PLAYER_NAME'] == player_name].copy()
    if pdf.empty:
        logging.info(f'  no qualified seasons for {player_name} — skipping career chart')
        return
    pdf['SEASON_INT'] = pdf['SEASON'].astype(int)
    pdf = pdf.sort_values('SEASON_INT')

    panels = [('PPG',    'PPG_Z',    'Points / game',         False),
              ('RPG',    'RPG_Z',    'Rebounds / game',       False),
              ('APG',    'APG_Z',    'Assists / game',        False),
              ('TS_PCT', 'TS_PCT_Z', 'True Shooting %',       True)]
    fig, axes = plt.subplots(2, 2, figsize=(12, 7))

    for ax, (stat, zs, title, is_pct) in zip(axes.flat, panels):
        ax.bar(pdf['SEASON'], pdf[stat], alpha=0.4, color=config.WNBA_TEAL)
        ax.set_ylabel(stat, color=config.WNBA_TEAL, fontsize=9)
        if is_pct:
            ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))

        ax2 = ax.twinx()
        ax2.plot(pdf['SEASON'], pdf[zs], marker='o',
                 color=config.WNBA_ORANGE, linewidth=2)
        ax2.axhline(0, linestyle='--', linewidth=0.7, color='gray', alpha=0.6)
        ax2.set_ylabel('Z-Score', color=config.WNBA_ORANGE, fontsize=9)

        ax.set_title(title, fontsize=10)
        ax.tick_params(axis='x', rotation=45, labelsize=7)

    plt.suptitle(f'Career Trajectory: {player_name}',
                 fontsize=14, fontweight='bold')
    plt.tight_layout()
    safe = player_name.replace("'", '').replace(' ', '_')
    helpers.save_chart(fig, config.OUTPUT_DIR, f'career_multistat_{safe}.png')
    plt.close(fig)


def chart_league_scoring_trend(clean_logs: pd.DataFrame):
    """League scoring trend (mean PTS per player-game) by season."""
    trend = clean_logs.groupby('SEASON')['PTS'].mean().reset_index()
    trend['SEASON_INT'] = trend['SEASON'].astype(int)
    trend = trend.sort_values('SEASON_INT')

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(trend['SEASON'], trend['PTS'], marker='o',
            color=config.WNBA_TEAL, linewidth=2.5)
    ax.fill_between(trend['SEASON'], trend['PTS'], alpha=0.12, color=config.WNBA_TEAL)
    ax.set_xticks(trend['SEASON'][::2])
    ax.set_ylabel('Avg points per player-game')
    ax.set_title('League-Wide Scoring Trend (1997–2024)',
                 fontsize=14, fontweight='bold')
    plt.tight_layout()
    helpers.save_chart(fig, config.OUTPUT_DIR, 'league_scoring_trend_historical.png')
    plt.close(fig)


# ── entry point ──────────────────────────────────────────────────────────────

def run(processed: dict):
    """Stage 4 — generate the historical visualizations."""
    logging.info('Stage 4 — Generating historical visualizations…')

    clean_logs = processed['clean_logs']
    qualified  = build_qualified_player_seasons(clean_logs, MIN_GAMES_QUALIFIED)

    chart_era_adjusted_dominance(qualified)
    chart_dominance_composite(qualified)
    chart_efficiency_volume(qualified)
    chart_three_point_revolution(clean_logs)
    chart_ppg_distribution_by_season(qualified)
    chart_league_scoring_trend(clean_logs)

    if 'clutch' in processed:
        chart_clutch_leaders(processed['clutch'])
    if 'consistency' in processed:
        chart_consistency_ranking(processed['consistency'])

    for player in config.KEY_PLAYERS:
        chart_player_multistat_career(player, qualified)

    logging.info(f'Stage 4 complete — historical charts saved to {config.OUTPUT_DIR}\n')