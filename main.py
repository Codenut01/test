import asyncio
import pandas as pd
from func_connections import connect_to_dydx
from constants import FIND_COINTEGRATED_PAIRS, PLACE_TRADES
from func_public import construct_market_prices, should_trade_based_on_time
from func_cointegration import store_cointegration_results
from func_entry_pairs import open_positions
from func_exit_pairs import monitor_and_exit_trades
from func_messaging import send_message
from datetime import datetime, timedelta


async def calculate_cointegrated_pairs(client):
    """
    Helper function to calculate and store cointegrated pairs.

    Args:
        client: The dYdX client instance.

    Returns:
        bool: True if successful, False otherwise.
    """
    try:
        print("\nRecalculating cointegrated pairs...")
        df_market_prices = await construct_market_prices(client)
        store_results = await store_cointegration_results(df_market_prices)
        if store_results == "saved":
            df_csv = pd.read_csv("cointegrated_pairs.csv")
            print(f"Cointegrated pairs refreshed with {len(df_csv)} pairs")
            return True
    except Exception as e:
        print(f"Error in cointegration recalculation: {e}")
        await send_message(f"Error in cointegration recalculation: {e}")
        return False


async def main():
    """
    Main entry point for the trading bot.
    """
    # Send a startup message
    await send_message("Bot launch successful")

    # Connect to DYDX client
    try:
        print("Connecting to client...")
        client = await connect_to_dydx()
        print("Connection successful")
    except Exception as e:
        print(f"Error connecting to client: {e}")
        await send_message(f"Failed to connect to client: {e}")
        return

    # Initial calculation of cointegrated pairs
    if FIND_COINTEGRATED_PAIRS:
        success = await calculate_cointegrated_pairs(client)
        if not success:
            print("Failed to calculate cointegrated pairs. Exiting.")
            return

    # Track the last refresh time for cointegrated pairs
    last_refresh = datetime.now()

    # Start monitoring trades in the background
    asyncio.create_task(monitor_and_exit_trades(client=client))

    # Run the bot continuously
    while True:
        try:
            # Check if within trading hours
            if not await should_trade_based_on_time():
                print("Outside trading hours. Pausing operations.")
                await asyncio.sleep(60)  # Check every minute
                continue

            # Recalculate cointegrated pairs every 10 minutes
            if datetime.now() - last_refresh > timedelta(minutes=10):
                if FIND_COINTEGRATED_PAIRS and await calculate_cointegrated_pairs(client):
                    last_refresh = datetime.now()

            # Place trades if allowed
            if PLACE_TRADES:
                print("Finding trading opportunities...")
                await open_positions(client)

        except Exception as e:
            print(f"Error in main trading loop: {e}")
            await send_message(f"Error in main trading loop: {e}")

        await asyncio.sleep(1)  # Small delay between iterations


if __name__ == "__main__":
    asyncio.run(main())
