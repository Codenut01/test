import time
import asyncio
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
        self.trades_file = "simulated_trades.csv"
        
        # Ensure trade log file exists with headers
        if not os.path.exists(self.trade_log_file):
            with open(self.trade_log_file, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow([
                    "Timestamp", "Market", "Side", "Size", 
                    "Entry Price", "Take Profit Price", 
                    "Trading Fee", "Simulated Profit/Loss", 
                    "Current Balance"
                ])
        
        # Ensure trades file exists with headers
        if not os.path.exists(self.trades_file):
            with open(self.trades_file, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow([
                    "Market", "Side", "Size", 
                    "Entry Price", "Take Profit Price"
                ])

    def simulate_trade_fee(self):
        """Simulate trading fees: 3/5 trades at $0.01, 2/5 trades at $0.02"""
        return 0.01 if random.random() < 0.6 else 0.02

    def is_market_open(self, market):
        """Check if market already has an open position"""
        return market in self.active_positions

    def log_trade(self, market, side, size, entry_price, take_profit_price):
        """Log trade details and simulate trade outcome"""
        trading_fee = self.simulate_trade_fee()
        
        # Simulate trade outcome (simplified)
        trade_outcome = size * (take_profit_price - entry_price) / entry_price if side == "BUY" else \
                        size * (entry_price - take_profit_price) / entry_price
        
        # Adjust balance
        trade_profit = trade_outcome - trading_fee
        self.balance += trade_profit

        # Track active positions
        self.active_positions[market] = {
            "side": side,
            "size": size,
            "entry_price": entry_price,
            "take_profit_price": take_profit_price
        }

        # Get current time in SAST
        sast = pytz.timezone('Africa/Johannesburg')
        current_time = datetime.now(sast).strftime("%Y-%m-%d %H:%M:%S")

        # Log to CSV
        with open(self.trade_log_file, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([
                current_time, market, side, size, 
                entry_price, take_profit_price, 
                trading_fee, trade_profit, self.balance
            ])
        
        # Log trade details
        with open(self.trades_file, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([
                market, side, size, 
                entry_price, take_profit_price
            ])

    def close_trade(self, market):
        """Close an active trade and update active positions"""
        if market in self.active_positions:
            del self.active_positions[market]

    def check_free_collateral(self):
        return self.balance > USD_MIN_COLLATERAL

    def is_max_positions(self):
        """Check if max positions limit is reached"""
        return len(self.active_positions) >= self.max_positions

# Global trade simulator
trade_simulator = TradeSimulator()

async def place_market_order(market_order, side, size, price, reduce_only):
    """Simulate market order placement without actual execution"""
    try:
        # Log the simulated trade
        trade_simulator.log_trade(
            market=market_order, 
            side=side, 
            size=size, 
            entry_price=price, 
            take_profit_price=price * 1.01  # Simple take profit calculation
        )
        
        print(f"Simulated order placed for {market_order}")
        return "Simulated Order"
    except Exception as e:
        print(f"Simulation error for {market_order}: {e}")
        raise MarketOrderError(f"Simulated order failed for {market_order}")

def free_colleteral(client):
    """Use trade simulator to check free collateral"""
    return trade_simulator.check_free_collateral()

def is_open_positions(client, market):
    """Check if a specific market has an open position"""
    return trade_simulator.is_market_open(market)

def is_max_positions(client):
    """Check if max positions limit is reached"""
    return trade_simulator.is_max_positions()