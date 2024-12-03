import asyncio
import json
import pandas as pd
import os
import shutil
from constants import ZSCORE_THRESH, USD_PER_TRADE, TP_AMOUNT
from func_utils import format_number
from func_public import get_candles_recent
from func_private import (
    trade_simulator,
    place_market_order,
    MarketOrderError,
)
from func_cointegration import calculate_zscore
from func_messaging import send_message


async def calculate_take_profit(tick_size, side, size, accept_price):
    """
    Calculate the take-profit price based on TP_AMOUNT and trade size.
    """
    tick1_size = float(tick_size)
    accept_price1 = float(accept_price)
    price1 = await format_number(accept_price1, tick1_size)
    price = float(price1)

    # Calculate the price change for the desired profit (TP_AMOUNT)
    price_change0 = TP_AMOUNT / float(size)
    price_change1 = await format_number(price_change0, tick1_size)
    price_change = float(price_change1)

    # Adjust the price for the trade direction
    take_profit = price + price_change if side == "BUY" else price - price_change

    return take_profit


async def save_trade_to_file_safe(trade_details, file_path="active_trades.json"):
    """
    Safely save trade details to a JSON file with data validation.
    """
    backup_path = file_path + ".bak"
    try:
        if os.path.exists(file_path):
            shutil.copy(file_path, backup_path)

        try:
            with open(file_path, "r") as file:
                trades = json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            trades = []

        trades.append(trade_details)

        with open(file_path, "w") as file:
            json.dump(trades, file, indent=4)
    except Exception as e:
        print(f"Error saving trade to file: {e}")


async def manage_trade_open(client, base_market, quote_market, z_score):
    """
    Handle opening a trade for a cointegrated pair.
    """
    is_base_open = trade_simulator.is_market_open(base_market)
    is_quote_open = trade_simulator.is_market_open(quote_market)
    max_positions = trade_simulator.is_max_positions()

    if not (is_base_open or is_quote_open) and not max_positions:
        base_side = "BUY" if z_score < 0 else "SELL"
        quote_side = "BUY" if z_score > 0 else "SELL"

        # Base market parameters
        market_base = await client.markets.get_perpetual_markets(market=base_market)
        oracle_base_price = float(market_base["markets"][base_market]["oraclePrice"])
        base_tick_size = float(market_base["markets"][base_market]["tickSize"])
        base_size = await format_number(
            1 / oracle_base_price * USD_PER_TRADE,
            float(market_base["markets"][base_market]["stepSize"])
        )
        base_price = oracle_base_price * 1.05 if base_side == "BUY" else oracle_base_price * 0.95
        base_take_profit = await calculate_take_profit(base_tick_size, base_side, base_size, oracle_base_price)

        # Quote market parameters
        market_quote = await client.markets.get_perpetual_markets(market=quote_market)
        oracle_quote_price = float(market_quote["markets"][quote_market]["oraclePrice"])
        quote_tick_size = float(market_quote["markets"][quote_market]["tickSize"])
        quote_size = await format_number(
            1 / oracle_quote_price * USD_PER_TRADE,
            float(market_quote["markets"][quote_market]["stepSize"])
        )
        quote_price = oracle_quote_price * 1.05 if quote_side == "BUY" else oracle_quote_price * 0.95
        quote_take_profit = await calculate_take_profit(quote_tick_size, quote_side, quote_size, oracle_quote_price)

        # Format prices
        accept_base_price = await format_number(base_price, base_tick_size)
        accept_quote_price = await format_number(quote_price, quote_tick_size)

        if trade_simulator.check_free_collateral():
            try:
                # Place base market order
                await place_market_order(
                    market_order=base_market,
                    side=base_side,
                    size=base_size,
                    price=float(accept_base_price),
                    reduce_only=False,
                )
                await save_trade_to_file_safe({
                    "market": base_market,
                    "side": base_side,
                    "size": base_size,
                    "take_profit_price": base_take_profit,
                    "tick_size": base_tick_size,
                })

                # Place quote market order
                await place_market_order(
                    market_order=quote_market,
                    side=quote_side,
                    size=quote_size,
                    price=float(accept_quote_price),
                    reduce_only=False,
                )
                await save_trade_to_file_safe({
                    "market": quote_market,
                    "side": quote_side,
                    "size": quote_size,
                    "take_profit_price": quote_take_profit,
                    "tick_size": quote_tick_size,
                })

                print(f"Order placed for {base_market}/{quote_market}")
                await send_message(f"Order placed for {base_market}/{quote_market}")

            except MarketOrderError as e:
                print(f"Error placing market order: {e}")
                await send_message(f"Error placing market order: {e}")


async def open_positions(client):
    """
    Manage opening positions based on cointegrated pairs.
    """
    try:
        df = pd.read_csv("cointegrated_pairs.csv")
    except Exception as e:
        print(f"Error reading cointegrated pairs file: {e}")
        await send_message(f"Error reading cointegrated pairs file: {e}")
        return

    await asyncio.sleep(0.5)

    for row in df.itertuples():
        base_market = row.base_market
        quote_market = row.quote_market
        hedge_ratio = row.hedge_ratio

        try:
            # Fetch recent candles for both markets
            series_1, series_2 = await asyncio.gather(
                get_candles_recent(client, base_market),
                get_candles_recent(client, quote_market)
            )

            if len(series_1) > 0 and len(series_1) == len(series_2):
                spread = series_1 - (hedge_ratio * series_2)
                # Await the calculate_zscore function
                z_score_series = await calculate_zscore(spread)
                z_score = z_score_series.iloc[-1]  # Use iloc for the last Z-score value

                # Check Z-score threshold
                if abs(z_score) >= ZSCORE_THRESH:
                    await manage_trade_open(client, base_market, quote_market, z_score)

        except Exception as e:
            print(f"Unexpected error for pair {base_market}/{quote_market}: {e}")
            await send_message(f"Unexpected error for pair {base_market}/{quote_market}: {e}")

    print("Finished processing positions.")
