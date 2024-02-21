import pandas as pd
import pytz
from APIconfig import client
from datetime import datetime, timedelta

def get_us_tickers():
    # Get a list of all exchanges
    exchanges = pd.DataFrame(client.get_exchanges(asset_class='stocks', locale='us'))

    # Filter for NASDAQ and NYSE exchanges only
    nasdaq_nyse_exchanges = exchanges[exchanges['mic'].isin(['XNAS', 'XNYS'])]  # MICs for NASDAQ and NYSE
    exchange_list = nasdaq_nyse_exchanges['mic'].tolist()

    us_tickers = []

    for exchange in exchange_list:
        try:
            # Fetch tickers with a higher limit, adjust as needed
            for ticker in client.list_tickers(market='stocks', exchange=exchange, limit=1000):
                if ticker.ticker.isalpha() and ticker.ticker.isupper():
                    us_tickers.append(ticker.ticker)
        except Exception as e:
            print(f"Error fetching tickers for exchange {exchange}: {e}")

    return us_tickers


def find_filtered_tickers(client, tickers, min_gap_up=0.08, max_gap_up=0.70, min_market_cap=500_000_000, min_premarket_volume=500_000):
    end_date = datetime.today()
    start_date = end_date - timedelta(days=1825)  # Last 5 years
    filtered_data = []
    eastern = pytz.timezone('US/Eastern')

    for index, ticker in enumerate(tickers, start=1):
        try:
            print(f"Processing {index} of {len(tickers)}: {ticker}")
            historical_data = client.get_aggs(ticker, 1, "day", start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
            ticker_details = client.get_ticker_details(ticker)
            
            for i in range(1, len(historical_data)):
                previous_day = historical_data[i - 1]
                current_day = historical_data[i]
                gap_up_percentage = (current_day.open / previous_day.close) - 1
                
                 # New conditions
                if (ticker_details.market_cap >= min_market_cap and 
                    min_gap_up <= gap_up_percentage < max_gap_up and 
                    3 <= current_day.open <= 99 and 
                    current_day.open > previous_day.high):

                    # Define premarket start and end times in milliseconds
                    premarket_start = datetime.fromtimestamp(current_day.timestamp / 1000).replace(hour=4, minute=0, second=0, tzinfo=pytz.timezone('US/Eastern'))
                    premarket_end = datetime.fromtimestamp(current_day.timestamp / 1000).replace(hour=9, minute=29, second=59, tzinfo=pytz.timezone('US/Eastern'))

                    # Fetch premarket aggregate data
                    premarket_start_ms = int(premarket_start.timestamp() * 1000)
                    premarket_end_ms = int(premarket_end.timestamp() * 1000)
                    premarket_data = client.get_aggs(ticker, 1, "minute", premarket_start_ms, premarket_end_ms)

                    # Process premarket data to find high, low, and volume
                    premarket_high = max([minute_data.high for minute_data in premarket_data])
                    premarket_low = min([minute_data.low for minute_data in premarket_data])
                    premarket_volume = sum([minute_data.volume for minute_data in premarket_data])

                    # Define times for OPEN prices
                    open_times = [datetime.fromtimestamp(current_day.timestamp / 1000, eastern).replace(hour=hour, minute=minute) for hour, minute in [(10, 0), (10, 30), (11, 30), (12, 30)]]

                    # Find OPEN prices at specific times
                    open_prices = {}
                    for open_time in open_times:
                        open_time_ms = int(open_time.timestamp() * 1000)
                        minute_data = client.get_aggs(ticker, 1, "minute", open_time_ms, open_time_ms)
                        key = open_time.strftime("%H:%M")
                        open_prices[key] = minute_data[0].open

                    # Define timerange for LOW price
                    low_start930 = datetime.fromtimestamp(current_day.timestamp / 1000, eastern).replace(hour=9, minute=30)
                    low_end = datetime.fromtimestamp(current_day.timestamp / 1000, eastern).replace(hour=10, minute=15)

                    low_start_ms = int(low_start930.timestamp() * 1000)
                    low_end_ms = int(low_end.timestamp() * 1000)

                     # Fetch LOW price data between 9:30 AM and 10:15 AM
                    low_data = client.get_aggs(ticker, 1, "minute", low_start_ms, low_end_ms)
                    low_price = min([bar.low for bar in low_data])

                    # Define timerange for LOW price (9:30 AM to 11:30 AM)
                    low_end1130 = datetime.fromtimestamp(current_day.timestamp / 1000, eastern).replace(hour=11, minute=30)

                    low_start_ms_extended = int(low_start930.timestamp() * 1000)
                    low_end_ms_extended = int(low_end1130.timestamp() * 1000)

                    # Fetch LOW price data between 9:30 AM and 11:30 AM
                    low_data_extended = client.get_aggs(ticker, 1, "minute", low_start_ms_extended, low_end_ms_extended)
                    low_price_extended = min([bar.low for bar in low_data_extended]) if low_data_extended else None


                    if premarket_volume*current_day.open >= 1_000_000 and premarket_volume >= min_premarket_volume:
                        # Fetch additional data for each ticker
                        filtered_info = {
                            'Ticker': ticker,
                            'Date': datetime.fromtimestamp(current_day.timestamp / 1000).strftime('%Y-%m-%d'),
                            'Given Market Cap': ticker_details.market_cap,
                            'Calculated Market Cap': ticker_details.share_class_shares_outstanding*previous_day.close,
                            'Open': current_day.open,
                            'High': current_day.high,
                            'Low': current_day.low,
                            'Close': current_day.close,
                            'LOW Price 9:30-10:15': low_price,
                            'LOW Price 9:30-11:30': low_price_extended,
                            'Previous Close': previous_day.close,
                            'Premarket High': premarket_high,
                            'Premarket Low': premarket_low,
                            'Premarket Volume': premarket_volume,
                        }

                        # Separate OPEN Prices into different columns
                        for time, price in open_prices.items():
                            filtered_info[f'OPEN Price {time}'] = price

                        filtered_data.append(filtered_info)
                        print(f"  Found gap up >= 8% and fulfilled remaining requirements for {ticker} on {filtered_info['Date']}")
                        break  # Skip remaining days once a gap up is found
        except Exception as e:
            print(f"Error processing ticker {ticker}: {e}")

    return filtered_data

# Get tickers from NASDAQ and NYSE
us_tickers = get_us_tickers()

# Find tickers with gap up
filtered_tickers_data = find_filtered_tickers(client, us_tickers)

# Process or print the gap up data
for data in filtered_tickers_data:
    print(data)

# Convert the filtered tickers data to a pandas DataFrame
filtered_tickers_df = pd.DataFrame(filtered_tickers_data)

# Specify the file path and name for the Excel file
excel_file_path = 'Excel/filtered_tickers_data.xlsx'

# Write the DataFrame to an Excel file
filtered_tickers_df.to_excel(excel_file_path, index=False)

print(f"Data stored in Excel file at: {excel_file_path}")




