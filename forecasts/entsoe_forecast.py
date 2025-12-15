# /// script
# dependencies = [
#   "entsoe-py",
#   "pandas",
# ]
# ///
import argparse
import json
import pandas as pd
from entsoe import EntsoePandasClient


def collect_prices(
    start_time: str,
    end_time: str,
    country_code: str,
    api_token: str,
) -> pd.Series:
    """
    Query ENTSO-E Day-Ahead prices for the window [start_time, end_time).

    Returns
    -------
    pandas.Series indexed by UTC timestamps, values in EUR/MWh.
    """
    client = EntsoePandasClient(api_key=api_token)
    start = pd.Timestamp(start_time, tz="UTC")
    end   = pd.Timestamp(end_time,   tz="UTC")
    return client.query_day_ahead_prices(country_code, start=start, end=end)


def main(start_time: str, end_time: str, country_code: str, api_token: str) -> None:
    prices = collect_prices(start_time, end_time, country_code, api_token)

    prices = prices.tz_convert("UTC")

    # 1. make sure the index is monotonic just in case
    prices = prices.sort_index()

    # 2. build an ordered list of pairs
    payload = [
        (ts.strftime("%Y-%m-%dT%H:%M:%S"), float(price))
        for ts, price in prices.items()          # ← Series.items() keeps order
    ]

    print(json.dumps(payload))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch ENTSO-E Day-Ahead electricity prices."
    )
    parser.add_argument("start_time",   help="UTC 'YYYY-MM-DD HH:MM' (00:00)")
    parser.add_argument("end_time",     help="UTC 'YYYY-MM-DD HH:MM' (next 00:00)")
    parser.add_argument("country_code", help="Bidding-zone (FI, DE_LU, DK1 …)")
    parser.add_argument("api_token",    help="Transparency Platform token")

    args = parser.parse_args()
    main(args.start_time, args.end_time, args.country_code, args.api_token)
