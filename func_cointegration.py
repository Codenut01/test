import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels.tsa.stattools import coint
import asyncio
from constants import MAX_HALF_LIFE, WINDOW


# Calculate Half-Life
async def calculate_half_life(spread):
    """
    Calculate the half-life of the spread using OLS.
    """
    try:
        df_spread = pd.DataFrame(spread, columns=["spread"])
        spread_lag = df_spread.shift(1).fillna(method="bfill")
        spread_ret = df_spread - spread_lag
        spread_lag2 = sm.add_constant(spread_lag)
        model = sm.OLS(spread_ret, spread_lag2).fit()
        half_life = int(round(-np.log(2) / model.params[1], 0))

        # Validate half-life
        if half_life <= 0 or half_life > MAX_HALF_LIFE:
            return None

        return half_life

    except Exception as e:
        print(f"Error calculating half-life: {e}")
        return None


# Calculate Z-Score
async def calculate_zscore(spread):
    """
    Calculate the rolling Z-score of the spread.
    """
    try:
        spread_series = pd.Series(spread)
        mean = spread_series.rolling(window=WINDOW).mean()
        std = spread_series.rolling(window=WINDOW).std()
        zscore = (spread_series - mean) / std

        # Drop NaN values from the Z-score
        zscore = zscore.dropna()
        return zscore

    except Exception as e:
        print(f"Error calculating Z-score: {e}")
        return pd.Series([])  # Return empty series


# Calculate Cointegration
async def calculate_cointegration(series_1, series_2):
    """
    Calculate the cointegration between two series.
    """
    try:
        series_1, series_2 = np.array(series_1).astype(float), np.array(series_2).astype(float)

        # Validate input series
        if len(series_1) == 0 or len(series_2) == 0:
            print("Error: One or both series are empty.")
            return 0, None, None

        if np.all(series_1 == series_1[0]) or np.all(series_2 == series_2[0]):
            print("Error: One or both series are constant.")
            return 0, None, None

        # Perform cointegration test
        coint_res = coint(series_1, series_2)
        p_value, coint_t, critical_value = coint_res[1], coint_res[0], coint_res[2][1]

        # Validate cointegration
        if p_value < 0.05 and coint_t < critical_value:
            model = sm.OLS(series_1, series_2).fit()
            hedge_ratio = model.params[0]
            spread = series_1 - hedge_ratio * series_2
            half_life = await calculate_half_life(spread)

            if half_life is not None and 0.1 <= hedge_ratio <= 5:
                return 1, hedge_ratio, half_life

        return 0, None, None

    except Exception as e:
        print(f"Error during cointegration test: {e}")
        return 0, None, None


async def store_cointegration_results(df_market_prices):
    """
    Identify and save cointegrated pairs from market data.
    Args:
        df_market_prices (DataFrame): Historical close prices for all markets.
    Returns:
        str: "saved" if successful, "failed" otherwise.
    """
    if df_market_prices.empty:
        print("Error: df_market_prices is empty. Ensure data is available.")
        return "failed"

    markets = df_market_prices.columns.tolist()
    if len(markets) < 2:
        print("Error: Not enough markets to calculate cointegration.")
        return "failed"

    criteria_met_pairs = []

    print("Finding Cointegrated Pairs...")
    try:
        for base_index, base_market in enumerate(markets[:-1]):
            for quote_index, quote_market in enumerate(markets[base_index + 1:]):
                series_1 = df_market_prices[base_market].dropna().values.tolist()
                series_2 = df_market_prices[quote_market].dropna().values.tolist()

                # Skip empty or constant series
                if not series_1 or not series_2:
                    print(f"Error: Empty data for {base_market}/{quote_market}, skipping.")
                    continue
                if np.all(series_1 == series_1[0]) or np.all(series_2 == series_2[0]):
                    print(f"Error: One or both series are constant for {base_market}/{quote_market}, skipping.")
                    continue

                try:
                    # Calculate cointegration
                    coint_flag, hedge_ratio, half_life = await calculate_cointegration(series_1, series_2)

                    # Log valid pairs
                    if coint_flag and hedge_ratio and half_life:
                        criteria_met_pairs.append({
                            "base_market": base_market,
                            "quote_market": quote_market,
                            "hedge_ratio": hedge_ratio,
                            "half_life": half_life
                        })

                except Exception as e:
                    print(f"Error processing pair {base_market}/{quote_market}: {e}")
                    continue

        # Save results
        if criteria_met_pairs:
            df_criteria_met = pd.DataFrame(criteria_met_pairs)
            df_criteria_met.to_csv("cointegrated_pairs.csv", index=False)
            print(f"Saved {len(criteria_met_pairs)} cointegrated pairs.")
        else:
            print("No cointegrated pairs found.")

    except Exception as e:
        print(f"Error during cointegration calculation: {e}")
        return "failed"

    print("Cointegrated Pairs Processing Completed.")
    return "saved"
