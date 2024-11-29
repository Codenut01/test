import json
import asyncio
from func_private import place_market_order
from func_public import get_oracle_price
from func_utils import format_number
import os
from func_connections import connect_to_dydx
from func_private import trade_simulator

client = connect_to_dydx()

async def monitor_and_exit_trades(file_path="active_trades.json", sleep_interval=1):
    """
    Monitor trades and simulate close orders if take-profit conditions are met.
    """
    # Ensure the file exists
    if not os.path.exists(file_path):
        print(f"{file_path} not found. Initializing file.")
        with open(file_path, "w") as file:
            json.dump([], file)

    while True:
        try:
            # Load trades from the file
            with open(file_path, "r") as file:
                trades = json.load(file)

            updated_trades = []

            for trade in trades:
                market = trade["market"]
                side = trade["side"]
                size = float(trade["size"])
                take_profit_price = float(trade["take_profit_price"])
                tick_size = float(trade["tick_size"])

                # Get the current oracle price
                current_price = float(get_oracle_price(client, market))

                # Calculate the formatted exit price based on side
                if side == "BUY":
                    exit_price = current_price * 0.95
                else:  # side == "SELL"
                    exit_price = current_price * 1.05

                formatted_exit_price = format_number(exit_price, tick_size)

                # Check if the take-profit condition is met
                if (side == "BUY" and current_price >= take_profit_price) or \
                   (side == "SELL" and current_price <= take_profit_price):
                    try:
                        await place_market_order(
                            market_order=market,
                            side="SELL" if side == "BUY" else "BUY",
                            size=size,
                            price=float(formatted_exit_price),
                            reduce_only=True,
                        )
                        # Close the trade in the simulator
                        trade_simulator.close_trade(market, float(formatted_exit_price))
                        print(f"Simulated exit trade for {market}")
                    except Exception as e:
                        print(f"Failed to simulate exit trade for {market}: {e}")
                        updated_trades.append(trade)
                else:
                    updated_trades.append(trade)

            # Save updated trades
            with open(file_path, "w") as file:
                json.dump(updated_trades, file, indent=4)

        except Exception as e:
            print(f"Error monitoring simulated trades: {e}")

        # Print current balance
        print(f"Current Simulated Balance: ${trade_simulator.balance:.2f}")

        await asyncio.sleep(sleep_interval)
