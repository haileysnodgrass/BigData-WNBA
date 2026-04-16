# ── helpers.py ────────────────────────────────────────────────────────────────
# Shared helper functions used across all pipeline stages.
# save_csv / save_parquet / save_chart each write locally and optionally
# upload to S3 — callers don't need to know which backend is active.
# ──────────────────────────────────────────────────────────────────────────────

import io
import os

import pandas as pd
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)
import config


# ── Directory setup ────────────────────────────────────────────────────────────

def ensure_local_dirs():
    """Create all local output directories if they don't exist."""
    for d in config.LOCAL_DIRS:
        os.makedirs(d, exist_ok=True)
    logging.info('Local directory structure ready.')


# ── S3 client (lazy-loaded) ───────────────────────────────────────────────────

_s3_client = None


def _get_s3():
    """Return a boto3 S3 client, initialising it once."""
    global _s3_client
    if _s3_client is None:
        try:
            import boto3
            _s3_client = boto3.client('s3')
        except ImportError:
            raise ImportError(
                "boto3 is required for S3 support. "
                "Install it with:  pip install boto3"
            )
    return _s3_client


def _s3_key(relative_path: str) -> str:
    """Build a full S3 key from a relative local path."""
    # Normalise path separators so Windows paths work too
    rel = relative_path.replace('\\', '/').lstrip('/')
    return f"{config.S3_PREFIX}/{rel}" if config.S3_PREFIX else rel


def _upload_bytes(data: bytes, s3_key: str, content_type: str = 'application/octet-stream'):
    """Upload raw bytes to S3."""
    _get_s3().put_object(
        Bucket=config.S3_BUCKET,
        Key=s3_key,
        Body=data,
        ContentType=content_type,
    )


# ── Save helpers ───────────────────────────────────────────────────────────────

def save_csv(df: pd.DataFrame, folder: str, filename: str, label: str = '') -> str:
    """
    Save a DataFrame as CSV.

    Writes to `folder/filename` locally (unless S3-only mode).
    If USE_S3 is True, also uploads to S3.

    Returns the local path (or S3 URI if local writing is skipped).
    """
    local_path = os.path.join(folder, filename)

    if not config.USE_S3 or config.S3_ALSO_SAVE_LOCAL:
        os.makedirs(folder, exist_ok=True)
        df.to_csv(local_path, index=False)

    if config.USE_S3:
        if not config.S3_BUCKET:
            raise ValueError("S3_BUCKET_NAME env var is not set.")
        # Derive a relative key from the base dir
        rel = os.path.relpath(local_path, config.BASE_DIR)
        key = _s3_key(rel)
        csv_bytes = df.to_csv(index=False).encode('utf-8')
        _upload_bytes(csv_bytes, key, content_type='text/csv')
        location = f"s3://{config.S3_BUCKET}/{key}"
    else:
        location = local_path

    tag = label or filename
    logging.info(f'  {tag:<45} {len(df):>6,} rows  →  {location}')
    return location


def save_parquet(df: pd.DataFrame, folder: str, filename: str, label: str = '') -> str:
    """
    Save a DataFrame as Parquet.

    Writes locally and/or uploads to S3 depending on config.
    Returns the local path or S3 URI.
    """
    local_path = os.path.join(folder, filename)

    if not config.USE_S3 or config.S3_ALSO_SAVE_LOCAL:
        os.makedirs(folder, exist_ok=True)
        df.to_parquet(local_path, index=False)

    if config.USE_S3:
        if not config.S3_BUCKET:
            raise ValueError("S3_BUCKET_NAME env var is not set.")
        rel = os.path.relpath(local_path, config.BASE_DIR)
        key = _s3_key(rel)
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        _upload_bytes(buf.getvalue(), key, content_type='application/octet-stream')
        location = f"s3://{config.S3_BUCKET}/{key}"
    else:
        location = local_path

    tag = label or filename
    logging.info(f'  {tag:<45} {len(df):>6,} rows  →  {location}')
    return location


def save_chart(fig, folder: str, filename: str) -> str:
    """
    Save a matplotlib figure as PNG.

    Writes locally and/or uploads to S3 depending on config.
    Returns the local path or S3 URI.
    """
    local_path = os.path.join(folder, filename)

    if not config.USE_S3 or config.S3_ALSO_SAVE_LOCAL:
        os.makedirs(folder, exist_ok=True)
        fig.savefig(local_path, bbox_inches='tight')

    if config.USE_S3:
        if not config.S3_BUCKET:
            raise ValueError("S3_BUCKET_NAME env var is not set.")
        rel = os.path.relpath(local_path, config.BASE_DIR)
        key = _s3_key(rel)
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight')
        _upload_bytes(buf.getvalue(), key, content_type='image/png')
        location = f"s3://{config.S3_BUCKET}/{key}"
    else:
        location = local_path

    logging.info(f'  {filename}  →  {location}')
    return location


# ── DataFrame helpers ──────────────────────────────────────────────────────────

def standardize_season(df: pd.DataFrame, column: str = 'SEASON') -> pd.DataFrame:
    """Force season values to clean strings like '2024'."""
    if column in df.columns:
        df[column] = df[column].astype(str).str.strip()
    return df


def season_slice(df: pd.DataFrame, season: str, column: str = 'SEASON') -> pd.DataFrame:
    """Return rows matching one season, with consistent type handling."""
    if column not in df.columns:
        raise KeyError(f"Column '{column}' not found in DataFrame.")
    return df[df[column].astype(str).str.strip() == str(season).strip()].copy()


def require_rows(df: pd.DataFrame, label: str):
    """record a warning if a DataFrame is unexpectedly empty."""
    if df.empty:
        logging.warning(f'⚠️  {label} is empty — check upstream fetch.')
    else:
        logging.info(f'✅ {label}: {len(df):,} rows')