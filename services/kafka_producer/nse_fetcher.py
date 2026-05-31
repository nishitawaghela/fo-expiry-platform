"""
nse_fetcher.py
Fetches live NSE F&O options chain data for Nifty 50 using pnsea.
pnsea handles session cookies and NSE rate limiting automatically.
"""

import pandas as pd
from datetime import datetime
from pnsea import NSE
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SYMBOL = "NIFTY"


def get_nearest_expiry(all_expiries: list) -> str:
    """
    Returns the nearest upcoming expiry from the list.
    pnsea returns expiries as strings like '02-Jun-2026'.
    """
    today = datetime.today().date()
    for expiry in all_expiries:
        expiry_date = datetime.strptime(expiry, "%d-%b-%Y").date()
        if expiry_date >= today:
            logger.info(f"Nearest expiry: {expiry}")
            return expiry
    raise ValueError("No upcoming expiry dates found.")


def parse_chain(df: pd.DataFrame, expiry: str, spot: float) -> list:
    """
    Transforms the wide-format DataFrame (one row per strike, CE+PE side by side)
    into a list of dicts — one record per strike per option type (CE and PE separately).
    This is the format we publish to Kafka.
    """
    records = []
    fetched_at = datetime.utcnow().isoformat()

    for _, row in df.iterrows():
        strike = row["strikePrice"]

        # Call option (CE)
        records.append({
            "symbol": SYMBOL,
            "expiry": expiry,
            "strike": float(strike),
            "option_type": "CE",
            "open_interest": int(row["CE_openInterest"] or 0),
            "oi_change": int(row["CE_changeinOpenInterest"] or 0),
            "implied_volatility": float(row["CE_impliedVolatility"] or 0),
            "last_price": float(row["CE_lastPrice"] or 0),
            "volume": int(row["CE_totalTradedVolume"] or 0),
            "bid_price": float(row["CE_bidprice"] or 0) if row["CE_bidprice"] else 0,
            "ask_price": float(row["CE_askPrice"] or 0) if row["CE_askPrice"] else 0,
            "spot_price": float(spot),
            "fetched_at": fetched_at,
        })

        # Put option (PE)
        records.append({
            "symbol": SYMBOL,
            "expiry": expiry,
            "strike": float(strike),
            "option_type": "PE",
            "open_interest": int(row["PE_openInterest"] or 0),
            "oi_change": int(row["PE_changeinOpenInterest"] or 0),
            "implied_volatility": float(row["PE_impliedVolatility"] or 0),
            "last_price": float(row["PE_lastPrice"] or 0),
            "volume": int(row["PE_totalTradedVolume"] or 0),
            "bid_price": float(row["PE_bidprice"] or 0) if row["PE_bidprice"] else 0,
            "ask_price": float(row["PE_askPrice"] or 0) if row["PE_askPrice"] else 0,
            "spot_price": float(spot),
            "fetched_at": fetched_at,
        })

    logger.info(f"Parsed {len(records)} records ({len(df)} strikes x 2 option types)")
    return records


def fetch_fo_data() -> dict:
    """
    Main entry point. Returns:
    {
        "spot_price": float,
        "expiry": str,
        "all_expiries": list,
        "records": list of dicts  <- one per strike per option type
    }
    """
    nse = NSE()
    df, all_expiries, spot = nse.options.option_chain(SYMBOL)
    expiry = get_nearest_expiry(all_expiries)
    records = parse_chain(df, expiry, spot)

    return {
        "spot_price": spot,
        "expiry": expiry,
        "all_expiries": all_expiries,
        "records": records,
    }


if __name__ == "__main__":
    data = fetch_fo_data()
    print(f"\nSpot Price   : {data['spot_price']}")
    print(f"Expiry       : {data['expiry']}")
    print(f"All expiries : {data['all_expiries'][:4]}")
    print(f"Total records: {len(data['records'])}")
    print("\nSample CE record:")
    print(data["records"][0])
    print("\nSample PE record:")
    print(data["records"][1])