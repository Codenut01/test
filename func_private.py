import random
import csv
import os
from datetime import datetime
import pytz
from constants import USD_MIN_COLLATERAL

class MarketOrderError(Exception):
    pass

class TradeSimulator:
    def __init__(self, initial_balance=130, max_positions=12):
        self.balance = initial_balance
        self.max_positions = max_positions
        self.active_positions = {}
        self.trade_log_file = "trade_simulation_log.csv"

        # Ensure trade log file exists with headers
        if not os.path.exists(self.trade_log_file):
            with open(self.trade_log_file, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow([
                    "Timestamp", "Market", "Side", "Size",
                    "Entry Price", "Take Profit Price",
                    "Exit Price", "Trading Fee",
                    "Simulated Profit/Loss", "Current Balance"
                ])

    def simulate_trade_fee(self):
        """
        Simulate trading fees:
        - 3/5 trades incur $0.01 fee.
        - 2/5 trades incur $0.02 fee.
        """
        return 0.01 if random.random() < 0.6 else 0.02

    def log_entry_trade(self, market, side, size, entry_price, take_profit_price):
        """
        Log the details of a trade when it is first opened.
        """
        trading_fee = self.simulate_trade_fee()

        # Track the active position
        self.active_positions[market] = {
            "side": side,
            "size": size,
            "entry_price": entry_price,
            "take_profit_price": take_profit_price,
            "trading_fee": trading_fee
        }

        # Get current timestamp in SAST
        sast = pytz.timezone('Africa/Johannesburg')
        current_time = datetime.now(sast).strftime("%Y-%m-%d %H:%M:%S")

        # Log the trade details to CSV
        with open(self.trade_log_file, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([
                current_time, market, side, size,
                entry_price, take_profit_price,
                "N/A",  # Exit price not yet available
                trading_fee, "N/A",  # Profit/Loss not yet realized
                self.balance
            ])

    def close_trade(self, market, exit_price):
        """
        Simulate closing a trade and adjust the balance based on the exit price.
        """
        if market in self.active_positions:
            trade = self.active_positions.pop(market)
            side = trade["side"]
            size = trade["size"]
            entry_price = trade["entry_price"]
            take_profit_price = trade["take_profit_price"]
            trading_fee = trade["trading_fee"]

            # Calculate profit/loss
            profit_loss = (exit_price - entry_price) * size if side == "BUY" else \
                          (entry_price - exit_price) * size

            # Adjust balance with profit/loss minus trading fee
            self.balance += profit_loss - trading_fee

            # Log the closed trade details
            self._log_closed_trade(
                market, side, size, entry_price, take_profit_price,
                exit_price, trading_fee, profit_loss
            )

    def _log_closed_trade(self, market, side, size, entry_price, take_profit_price,
                          exit_price, trading_fee, profit_loss):
        """
        Log the details of a closed trade to the trade simulation log.
        """
        sast = pytz.timezone('Africa/Johannesburg')
        current_time = datetime.now(sast).strftime("%Y-%m-%d %H:%M:%S")

        with open(self.trade_log_file, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([
                current_time, market, side, size,
                entry_price, take_profit_price, exit_price,
                trading_fee, profit_loss, self.balance
            ])

    def check_free_collateral(self):
        """Check if sufficient free collateral is available."""
        return self.balance > USD_MIN_COLLATERAL

    def is_max_positions(self):
        """Check if the maximum number of open positions has been reached."""
        return len(self.active_positions) >= self.max_positions

    def is_market_open(self, market):
        """Check if a specific market already has an open position."""
        return market in self.active_positions


# Global trade simulator
trade_simulator = TradeSimulator()

async def place_market_order(market_order, side, size, price, reduce_only, take_profit_price=None):
    """
    Simulate placing a market order without actual execution.
    """
    try:
        if not reduce_only:
            # Log the entry trade with provided prices
            trade_simulator.log_entry_trade(
                market=market_order,
                side=side,
                size=size,
                entry_price=price,
                take_profit_price=take_profit_price
            )
        else:
            # Exit logic will rely on the provided price as the exit price
            trade_simulator.close_trade(
                market=market_order,
                exit_price=price
            )

        print(f"Simulated {'exit' if reduce_only else 'entry'} order placed for {market_order}")
        return "Simulated Order"
    except Exception as e:
        print(f"Simulation error for {market_order}: {e}")
        raise MarketOrderError(f"Simulated order failed for {market_order}")
