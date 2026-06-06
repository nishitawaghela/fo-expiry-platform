"""
anomaly_detector.py
Trains an Isolation Forest on historical OI shift data from Delta Lake.
Flags strikes where OI shift velocity is abnormally high —
a signal of unusual institutional positioning ahead of expiry.
"""

import os
import logging
import pandas as pd
import numpy as np
from deltalake import DeltaTable
from sklearn.ensemble import IsolationForest
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DELTA_LAKE_PATH = os.getenv("DELTA_LAKE_PATH", "../../data/delta_lake")


def load_historical_oi_shifts() -> pd.DataFrame:
    """
    Reads all OI shift records from Delta Lake.
    Returns a DataFrame with strike, option_type, oi_shift columns.
    """
    delta_path = os.path.abspath(DELTA_LAKE_PATH)

    try:
        dt = DeltaTable(delta_path)
        df = dt.to_pandas()
        logger.info(f"Loaded {len(df)} records from Delta Lake")
        return df
    except Exception as e:
        logger.error(f"Failed to read Delta Lake: {e}")
        return pd.DataFrame()


def train_isolation_forest(df: pd.DataFrame) -> IsolationForest:
    """
    Trains Isolation Forest on OI shift features.
    Features used:
      - oi_shift: how much OI changed since last snapshot
      - open_interest: current OI level
      - implied_volatility: market's expectation of movement
    
    Isolation Forest works by randomly partitioning data and measuring
    how quickly it can isolate each point. Anomalies are isolated faster
    because they're far from the majority of data points.
    contamination=0.05 means we expect ~5% of records to be anomalous.
    """
    features = ["oi_shift", "open_interest", "implied_volatility"]

    # Drop rows with nulls in feature columns
    df_clean = df[features].dropna()

    if len(df_clean) < 10:
        logger.warning("Not enough data to train Isolation Forest — need at least 10 records")
        return None

    model = IsolationForest(
        n_estimators=100,       # number of trees in the forest
        contamination=0.05,     # expected proportion of anomalies
        random_state=42,        # reproducibility
        n_jobs=-1,              # use all CPU cores
    )

    model.fit(df_clean)
    logger.info(f"Isolation Forest trained on {len(df_clean)} records")
    return model


def detect_anomalies(df: pd.DataFrame, model: IsolationForest) -> pd.DataFrame:
    """
    Runs the trained model on the latest batch of records.
    Returns only the anomalous records with their anomaly score.

    IsolationForest.predict() returns:
      -1 = anomaly
       1 = normal

    IsolationForest.score_samples() returns the anomaly score —
    more negative = more anomalous.
    """
    features = ["oi_shift", "open_interest", "implied_volatility"]
    df_clean = df[features].fillna(0)

    predictions   = model.predict(df_clean)
    anomaly_scores = model.score_samples(df_clean)

    df = df.copy()
    df["is_anomaly"]    = predictions == -1
    df["anomaly_score"] = anomaly_scores

    anomalies = df[df["is_anomaly"]].copy()
    anomalies = anomalies.sort_values("anomaly_score")  # most anomalous first

    logger.info(f"Detected {len(anomalies)} anomalies out of {len(df)} records")
    return anomalies


def run_anomaly_detection() -> list:
    """
    Main entry point.
    Loads Delta Lake, trains model, detects anomalies in latest batch.
    Returns list of anomalous strike dicts for Redis caching.
    """
    df = load_historical_oi_shifts()

    if df.empty:
        logger.warning("No data in Delta Lake yet — skipping anomaly detection")
        return []

    # Train on all historical data
    model = train_isolation_forest(df)
    if model is None:
        return []

    # Detect anomalies in the latest batch only
    latest_batch = df[df["batch_id"] == df["batch_id"].max()]
    if latest_batch.empty:
        return []

    anomalies = detect_anomalies(latest_batch, model)

    # Format for Redis / API response
    results = []
    for _, row in anomalies.iterrows():
        results.append({
            "strike":        row.get("strike"),
            "option_type":   row.get("option_type"),
            "oi_shift":      int(row.get("oi_shift", 0)),
            "open_interest": int(row.get("open_interest", 0)),
            "anomaly_score": round(float(row.get("anomaly_score", 0)), 4),
            "expiry":        row.get("expiry"),
            "detected_at":   datetime.now(timezone.utc).isoformat(),
        })

    return results


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    anomalies = run_anomaly_detection()
    print(f"\nDetected {len(anomalies)} anomalies")
    for a in anomalies[:5]:
        print(a)