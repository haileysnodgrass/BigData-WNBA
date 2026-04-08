# ── analytics.py ──────────────────────────────────────────────────────────────
# Stage 4 — Analytics & Chart Generation
# Each function produces one chart and saves it via helpers.save_chart(),
# which transparently handles local disk and/or S3 upload.
# ──────────────────────────────────────────────────────────────────────────────

import warnings

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns

import config
import helpers

warnings.filterwarnings('ignore')
sns.set_theme(style='darkgrid', palette='muted')
plt.rcParams['figure.dpi'] = 130


def _season_team(df_team, season):
    return helpers.season_slice(df_team, season, column='SEASON')


def chart_top_scorers(df_player_stats, season=None):
    season = season or config.TARGET_SEASON
    df = helpers.season_slice(df_player_stats, season)
    top = df.nlargest(15, 'PPG')

    fig, ax = plt.subplots(figsize=(12, 6))
    bars = ax.barh(top['PLAYER_NAME'], top['PPG'], color=config.WNBA_ORANGE, edgecolor='white')
    ax.bar_label(bars, fmt='%.1f', padding=4, fontsize=9)
    ax.set_xlabel('Points Per Game')
    ax.set_title(f'Top 15 Scorers — {season} WNBA Season', fontsize=14, fontweight='bold')
    ax.invert_yaxis()
    plt.tight_layout()

    helpers.save_chart(fig, config.OUTPUT_DIR, f'top_scorers_{season}.png')
    plt.close(fig)


def chart_team_win_vs_scoring(df_team_stats, season=None):
    season = season or config.TARGET_SEASON
    df = _season_team(df_team_stats, season)
    if df.empty:
        print(f'⚠️  No team stats for {season}')
        return

    fig, ax = plt.subplots(figsize=(10, 6))
    sc = ax.scatter(df['PPG'], df['WIN_PCT'], s=120, c=df['OFF_EFF'],
                    cmap='viridis', edgecolors='white', linewidths=0.8, zorder=3)
    plt.colorbar(sc, ax=ax, label='Offensive Efficiency (PPG / (TPG+1))')

    for _, row in df.iterrows():
        ax.annotate(row['TEAM_ABBREVIATION'], (row['PPG'], row['WIN_PCT']),
                    xytext=(4, 4), textcoords='offset points', fontsize=8)

    ax.yaxis.set_major_formatter(mtick.PercentFormatter(xmax=1))
    ax.set_xlabel('Points Per Game')
    ax.set_ylabel('Win %')
    ax.set_title(f'Team Win % vs. Scoring — {season}', fontsize=14, fontweight='bold')
    plt.tight_layout()

    helpers.save_chart(fig, config.OUTPUT_DIR, f'team_win_vs_scoring_{season}.png')
    plt.close(fig)


def chart_team_efficiency(df_team_stats, season=None):
    season = season or config.TARGET_SEASON
    df = _season_team(df_team_stats, season).sort_values('OFF_EFF', ascending=True)
    if df.empty:
        return

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.barh(df['TEAM_ABBREVIATION'], df['OFF_EFF'],
                   color=config.WNBA_TEAL, edgecolor='white')
    ax.bar_label(bars, fmt='%.2f', padding=4, fontsize=9)
    ax.set_xlabel('Offensive Efficiency')
    ax.set_title(f'Team Offensive Efficiency — {season}', fontsize=14, fontweight='bold')
    plt.tight_layout()

    helpers.save_chart(fig, config.OUTPUT_DIR, f'team_efficiency_{season}.png')
    plt.close(fig)


def chart_monthly_scoring_heatmap(df_monthly, season=None):
    season = season or config.TARGET_SEASON
    df = helpers.season_slice(df_monthly, season)
    if df.empty:
        print(f'⚠️  No monthly scoring data for {season}')
        return

    pivot = df.pivot_table(index='SEASON', columns='MONTH', values='AVG_PTS')

    fig, ax = plt.subplots(figsize=(12, 3))
    sns.heatmap(pivot, annot=True, fmt='.1f', cmap='YlOrRd',
                linewidths=0.5, ax=ax, cbar_kws={'label': 'Avg PTS'})
    ax.set_title(f'Monthly Scoring Heatmap — {season}', fontsize=14, fontweight='bold')
    ax.set_xlabel('Month')
    plt.tight_layout()

    helpers.save_chart(fig, config.OUTPUT_DIR, f'monthly_scoring_heatmap_{season}.png')
    plt.close(fig)


def chart_top_game_performances(df_top_perf):
    top = df_top_perf.head(20)

    fig, ax = plt.subplots(figsize=(12, 7))
    colors = [config.WNBA_ORANGE if s == config.TARGET_SEASON else config.WNBA_TEAL
              for s in top['SEASON']]
    bars = ax.barh(
        top['PLAYER_NAME'] + ' (' + top['SEASON'].astype(str) + ')',
        top['PTS'], color=colors, edgecolor='white',
    )
    ax.bar_label(bars, fmt='%d', padding=4, fontsize=9)
    ax.set_xlabel('Points Scored')
    ax.set_title('Top 20 Single-Game Performances — 2018–2024', fontsize=14, fontweight='bold')
    ax.invert_yaxis()
    plt.tight_layout()

    helpers.save_chart(fig, config.OUTPUT_DIR, 'top_game_performances_2018_2024.png')
    plt.close(fig)


def chart_player_scoring_trend(player_name, df_player_stats):
    pdf = df_player_stats[df_player_stats['PLAYER_NAME'] == player_name].sort_values('SEASON')
    if pdf.empty:
        print(f'⚠️  No data for {player_name}')
        return

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(pdf['SEASON'], pdf['PPG'], marker='o', color=config.WNBA_ORANGE,
            linewidth=2.5, markersize=8, label='PPG')
    ax.fill_between(pdf['SEASON'], pdf['PPG'], alpha=0.15, color=config.WNBA_ORANGE)

    for _, row in pdf.iterrows():
        ax.annotate(f"{row['PPG']:.1f}", (row['SEASON'], row['PPG']),
                    xytext=(0, 8), textcoords='offset points', ha='center', fontsize=9)

    ax.set_xlabel('Season')
    ax.set_ylabel('Points Per Game')
    ax.set_title(f'{player_name} — PPG Trend 2018–2024', fontsize=14, fontweight='bold')
    ax.legend()
    plt.tight_layout()

    safe_name = player_name.replace("'", '').replace(' ', '_')
    helpers.save_chart(fig, config.OUTPUT_DIR, f'scoring_trend_{safe_name}_2018_2024.png')
    plt.close(fig)


def chart_ppg_trend_key_players(df_player_stats, players=None):
    players = players or config.KEY_PLAYERS

    found = [p for p in players if p in df_player_stats['PLAYER_NAME'].values]
    if not found:
        print('⚠️  Key players not found — using top 3 scorers overall.')
        found = (
            df_player_stats.groupby('PLAYER_NAME')['PPG']
            .mean().nlargest(3).index.tolist()
        )

    fig, ax = plt.subplots(figsize=(12, 5))
    for player in found:
        pdf = df_player_stats[df_player_stats['PLAYER_NAME'] == player].sort_values('SEASON')
        ax.plot(pdf['SEASON'], pdf['PPG'], marker='o', linewidth=2.5, markersize=7, label=player)

    ax.set_xlabel('Season')
    ax.set_ylabel('Points Per Game')
    ax.set_title('PPG Trend Across Seasons — Key Players 2018–2024', fontsize=14, fontweight='bold')
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()

    helpers.save_chart(fig, config.OUTPUT_DIR, 'ppg_trend_by_season_2018_2024.png')
    plt.close(fig)


def run(processed):
    """
    Run all charts in Stage 4.

    Parameters
    ----------
    processed : dict  — output from processing.run()
    """
    print('📊 Stage 4 — Generating charts...')

    df_player = processed['player_stats']
    df_team   = processed['team_stats']
    df_monthly= processed['monthly']
    df_top    = processed['top_perf']

    chart_top_scorers(df_player)
    chart_team_win_vs_scoring(df_team)
    chart_team_efficiency(df_team)
    chart_monthly_scoring_heatmap(df_monthly)
    chart_top_game_performances(df_top)
    chart_ppg_trend_key_players(df_player)

    for player in config.KEY_PLAYERS:
        chart_player_scoring_trend(player, df_player)

    print(f'✅ Stage 4 complete — charts saved to {config.OUTPUT_DIR}\n')
