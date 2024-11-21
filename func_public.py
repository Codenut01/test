'''
Connecting DYDX Public requests
'''
import time
from func_utils import get_ISO_times
import pandas as pd
import numpy as np
from constants import RESOLUTION
from datetime import datetime
import pytz


# Get revelant time periods for ISO from and to
ISO_TIMES = get_ISO_times()


# Get Candles Recent
def get_candles_recent(client, market):

    # Define output
    close_prices = []

    # Protect API
    time.sleep(0.2)

    # Get data
    candles = client.markets.get_perpetual_market_candles(
        market=market,
        resolution=RESOLUTION,
        limit=100
    )

    # Structure data
    for candle in candles.data['candles']:
        close_prices.append(candle["close"])

    # Construct and return close price series
    close_prices.reverse()
    prices_reuslt = np.array(close_prices).astype(np.float64)
    return prices_reuslt



# Get Candles Historical
def get_candles_historical(client, market):

  # Define output
  close_prices = []

  # Extract historical price data for each timeframe
  for timeframe in ISO_TIMES.keys():

    # Confirm times needed
    tf_obj = ISO_TIMES[timeframe]
    from_iso = tf_obj["from_iso"]
    to_iso = tf_obj["to_iso"]

    # Protect rate limits
    time.sleep(0.2)

    # Get data
    candles = client.markets.get_perpetual_market_candles(
      market=market,
      resolution=RESOLUTION,
      from_iso=from_iso,
      to_iso=to_iso,
      limit=100
    )

    # Structure data
    for candle in candles.data["candles"]:
      close_prices.append({"datetime": candle["startedAt"], market: candle["close"] })

  # Construct and return DataFrame
  close_prices.reverse()
  return close_prices



# Construct market prices
def construct_market_prices(client):

    # Declare variables
    tradeable_markets = []
    markets = client.markets.get_perpetual_markets()

    # Find tradeable pairs
    for market in markets.data['markets'].keys():
        market_info = markets.data['markets'][market]
        if market_info['status'] == 'ACTIVE' and market_info['marketType'] == 'CROSS':
            tradeable_markets.append(market)

    # Set initial DataFrame
    df = None
    for market in tradeable_markets:
        try:
            close_prices = get_candles_historical(client, market)
            if not close_prices:
                print(f"Skipping {market} due to missing data")
                continue

            df_add = pd.DataFrame(close_prices)
            if 'datetime' not in df_add.columns:
                print(f"Skipping {market} due to missing datetime column")
                continue

            df_add.set_index("datetime", inplace=True)

            if df is None:
                df = df_add
            else:
                df = pd.merge(df, df_add, how="outer", on="datetime", copy=False)
            
            print(f"Successfully added {market}")
        except Exception as e:
            print(f"Error processing {market}: {e}")
            continue

    if df is None:
        raise ValueError("No valid market data found")

    # Check any columns with NaNs
    nans = df.columns[df.isna().any()].tolist()
    if len(nans) > 0:
        print("Dropping columns with NaNs: ")
        print(nans)
        df.drop(columns=nans, inplace=True)

    # Return result
    print(df)
    return df

def should_trade_based_on_time():
    """
    Check if the current time in South African time (SAST) is within trading hours.

    Returns:
        bool: True if within trading hours, False otherwise.
    """
    # Get the current time in South Africa
    south_africa_tz = pytz.timezone("Africa/Johannesburg")
    current_time = datetime.now(south_africa_tz).time()

    # Define trading hours: 10:00 AM to midnight (SAST)
    start_time = datetime.strptime("10:00", "%H:%M").time()
    end_time = datetime.strptime("23:59", "%H:%M").time()  # Midnight

    return start_time <= current_time <= end_time

def get_oracle_price(client, market):

    # Protect API
    time.sleep(0.2)

    positions = client.markets.get_perpetual_markets(market=market).data
    oracle_price = positions["markets"][market]["oraclePrice"]

    return oracle_price
