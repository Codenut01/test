'''
Connecting DYDX Public requests
'''
import asyncio
import pandas as pd
import numpy as np
from constants import RESOLUTION
from datetime import datetime
import pytz
import time


# Get Candles Recent
async def get_candles_recent(client, market):

    # Define output
    close_prices = []

    # Protect API
    await asyncio.sleep(0.2)

    # Get data
    candles = await client.markets.get_perpetual_market_candles(
        market=market,
        resolution=RESOLUTION,
        limit=48
    )

    # Structure data
    for candle in candles['candles']:
        close_prices.append(candle["close"])

    # Construct and return close price series
    close_prices.reverse()
    prices_reuslt = np.array(close_prices).astype(np.float64)
    return prices_reuslt



async def get_candles_historical(client, market, limit=120):
    """
    Fetch historical market candles for a given market.
    Args:
        client: The dYdX client instance.
        market: The market (e.g., "BTC-USD") to fetch data for.
        limit: Number of candles to fetch (default: 100).
    Returns:
        List[Dict]: A list of dictionaries containing 'datetime' and 'close' price.
    """
    close_prices = []

    try:
        # Fetch candle data
        response = await client.markets.get_perpetual_market_candles(
            market=market,
            resolution="5MINS",
            limit=limit
        )

        # Validate response structure
        if "candles" not in response or not response["candles"]:
            print(f"No data returned for market {market}")
            return close_prices

        # Process each candle
        for candle in response["candles"]:
            close_prices.append({
                "datetime": candle["startedAt"],
                market: float(candle["close"])  # Ensure numeric close price
            })

        # Reverse to chronological order
        close_prices.reverse()

    except Exception as e:
        print(f"Error fetching historical candles for {market}: {e}")

    return close_prices



# Construct market prices
async def construct_market_prices(client):

    # Declare variables
    tradeable_markets = []
    markets = await client.markets.get_perpetual_markets()

    # Find tradeable pairs
    for market in markets['markets'].keys():
        market_info = markets['markets'][market]
        if market_info['status'] == 'ACTIVE' and market_info['marketType'] == 'CROSS':
            tradeable_markets.append(market)

    # Set initial DataFrame
    df = None
    for market in tradeable_markets:
        try:
            close_prices = await get_candles_historical(client, market)
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


async def should_trade_based_on_time():
    """
    Check if the current time in South African time (SAST) is within trading hours and not on weekends.

    Returns:
        bool: True if within trading hours and not on Saturday or Sunday, False otherwise.
    """
    # Get the current time in South Africa
    south_africa_tz = pytz.timezone("Africa/Johannesburg")
    now = datetime.now(south_africa_tz)
    current_time = now.time()
    current_day = now.weekday()  # Monday is 0, Sunday is 6

    # Define trading hours: 10:00 AM to midnight (SAST)
    start_time = datetime.strptime("10:00", "%H:%M").time()
    end_time = datetime.strptime("23:59", "%H:%M").time()  # Midnight

    # Return False if it's Saturday (5) or Sunday (6)
    if current_day >= 5:
        return False

    # Return True if the current time is within trading hours
    return start_time <= current_time <= end_time

async def get_oracle_price(client, market):

    # Protect API
    await asyncio.sleep(0.2)

    positions = await client.markets.get_perpetual_markets(market=market)
    oracle_price = positions["markets"][market]["oraclePrice"]

    return oracle_price
