import sys
import pandas as pd
import pytz
from APIconfig import client
from datetime import datetime, timedelta
import os

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
                    # if len(us_tickers) >= 100:  # Limit to 100 tickers
                    #     return us_tickers
        except Exception as e:
            print(f"Error fetching tickers for exchange {exchange}: {e}")

    return us_tickers


def fetch_premarket_data(client, ticker, current_day):
    # Define the premarket start and end times
    premarket_start = datetime.fromtimestamp(current_day.timestamp / 1000).replace(hour=4, minute=0, second=0, tzinfo=pytz.timezone('US/Eastern'))
    premarket_end = datetime.fromtimestamp(current_day.timestamp / 1000).replace(hour=9, minute=29, second=59, tzinfo=pytz.timezone('US/Eastern'))
    
    # Convert to milliseconds
    premarket_start_ms = int(premarket_start.timestamp() * 1000)
    premarket_end_ms = int(premarket_end.timestamp() * 1000)
    
    # Fetch premarket data
    return client.get_aggs(ticker, 1, "minute", premarket_start_ms, premarket_end_ms)

def fetch_open_prices(client, ticker, current_day):
    eastern = pytz.timezone('US/Eastern')
    # Define the open times for fetching prices
    open_times = [datetime.fromtimestamp(current_day.timestamp / 1000, eastern).replace(hour=hour, minute=minute) for hour, minute in [(10, 0)]]
    
    # Fetch open prices for each time
    open_prices = {}
    for open_time in open_times:
        open_time_ms = int(open_time.timestamp() * 1000)
        minute_data = client.get_aggs(ticker, 1, "minute", open_time_ms, open_time_ms)
        key = open_time.strftime("%H:%M")
        open_prices[key] = minute_data[0].open
    return open_prices

def fetch_low_price(client, ticker, current_day, hour, minute):
    eastern = pytz.timezone('US/Eastern')
    # Define the start and end times for the low price range
    low_start = datetime.fromtimestamp(current_day.timestamp / 1000, eastern).replace(hour=hour, minute=minute)
    low_end = low_start + timedelta(minutes=45)  # Assuming 45 minutes time range
    
    # Convert to milliseconds
    low_start_ms = int(low_start.timestamp() * 1000)
    low_end_ms = int(low_end.timestamp() * 1000)
    
    # Fetch low price data
    low_data = client.get_aggs(ticker, 1, "minute", low_start_ms, low_end_ms)
    return min([bar.low for bar in low_data])



def Gap_up_reversal_long(client, tickers, min_gap_up=0.20, min_premarket_volume=500_000):
    end_date = datetime.today()
    start_date = end_date - timedelta(days=1825)  # Last 5 years
    filtered_data = []
    eastern = pytz.timezone('US/Eastern')

    for index, ticker in enumerate(tickers, start=1):
        print(f"Processing {index} of {len(tickers)}: {ticker}")

        try:
            # Get historical data with error handling
            try:
                historical_data = client.get_aggs(ticker, 1, "day", start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
                if not historical_data or len(historical_data) < 2:
                    print(f"  Not enough historical data for {ticker}")
                    continue
            except Exception as e:
                print(f"  Error getting historical data for {ticker}: {str(e)}")
                continue

            # Get ticker details with error handling
            try:
                ticker_details = client.get_ticker_details(ticker)
                market_cap = ticker_details.share_class_shares_outstanding * historical_data[0].close
            except Exception as e:
                print(f"  Could not get details for {ticker}, setting market cap to None")
                market_cap = None
            
            for i in range(1, len(historical_data)):
                current_day = historical_data[i]
                current_date = datetime.fromtimestamp(current_day.timestamp / 1000).date()
                previous_day = historical_data[i - 1]
                
                try:
                    gap_up_percentage = ((current_day.open - previous_day.close) / previous_day.close)
                except ZeroDivisionError:
                    print(f"  Zero division error for {ticker} on {current_date}")
                    continue

                if gap_up_percentage >= min_gap_up:
                    # Premarket data (4:00 AM - 9:29 AM)
                    try:
                        premarket_start = datetime.fromtimestamp(current_day.timestamp / 1000).replace(
                            hour=4, minute=0, second=0, tzinfo=pytz.timezone('US/Eastern'))
                        premarket_end = datetime.fromtimestamp(current_day.timestamp / 1000).replace(
                            hour=9, minute=29, second=59, tzinfo=pytz.timezone('US/Eastern'))
                        premarket_start_ms = int(premarket_start.timestamp() * 1000)
                        premarket_end_ms = int(premarket_end.timestamp() * 1000)
                        
                        premarket_data = client.get_aggs(ticker, 1, "minute", premarket_start_ms, premarket_end_ms)
                        
                        if premarket_data:
                            premarket_high = max([minute_data.high for minute_data in premarket_data])
                            premarket_low = min([minute_data.low for minute_data in premarket_data])
                            premarket_volume = sum([minute_data.volume for minute_data in premarket_data])
                        else:
                            print(f"  No premarket data for {ticker} on {current_date}")
                            continue
                    except Exception as e:
                        print(f"  Error getting premarket data for {ticker} on {current_date}: {str(e)}")
                        continue

                    # Initialize dictionary for time window data
                    time_data = {}
                    
                    # Process all time windows (keep your exact time windows)
                    time_windows = [
                        ('9:30am-9:44am', 9, 30, 9, 44),
                        ('9:45am-9:59am', 9, 45, 9, 59),
                        ('10:00am-10:14am', 10, 0, 10, 14),
                        ('10:15am-11:30am', 10, 15, 11, 30),
                        ('10:15am-12:30pm', 10, 15, 12, 30),
                        ('10:15am-13:30pm', 10, 15, 13, 30)
                    ]
                    
                    for window_name, start_h, start_m, end_h, end_m in time_windows:
                        try:
                            window_start = datetime.fromtimestamp(current_day.timestamp / 1000, eastern).replace(
                                hour=start_h, minute=start_m)
                            window_end = datetime.fromtimestamp(current_day.timestamp / 1000, eastern).replace(
                                hour=end_h, minute=end_m)
                            
                            window_data = client.get_aggs(ticker, 1, "minute", 
                                                        int(window_start.timestamp() * 1000), 
                                                        int(window_end.timestamp() * 1000))
                            
                            if window_data:
                                time_data[f'{window_name} (high)'] = max([bar.high for bar in window_data])
                                time_data[f'{window_name} (close)'] = window_data[-1].close
                            else:
                                time_data[f'{window_name} (high)'] = None
                                time_data[f'{window_name} (close)'] = None
                        except Exception as e:
                            print(f"  Error processing window {window_name} for {ticker} on {current_date}: {str(e)}")
                            time_data[f'{window_name} (high)'] = None
                            time_data[f'{window_name} (close)'] = None

                    # Process open times (keep your exact open times)
                    open_times = [
                        ('11:30am', 11, 30),
                        ('12:30pm', 12, 30),
                        ('13:30pm', 13, 30)
                    ]
                    
                    for time_name, hour, minute in open_times:
                        try:
                            open_time = datetime.fromtimestamp(current_day.timestamp / 1000, eastern).replace(
                                hour=hour, minute=minute)
                            open_data = client.get_aggs(ticker, 1, "minute", 
                                                       int(open_time.timestamp() * 1000), 
                                                       int(open_time.timestamp() * 1000))
                            
                            if open_data:
                                time_data[f'{time_name} (open)'] = open_data[0].open
                            else:
                                time_data[f'{time_name} (open)'] = None
                        except Exception as e:
                            print(f"  Error getting open price at {time_name} for {ticker} on {current_date}: {str(e)}")
                            time_data[f'{time_name} (open)'] = None

                    # Process low prices (9:30am-10:15am and 9:30am-1:30pm)
                    try:
                        low_start_930 = datetime.fromtimestamp(current_day.timestamp / 1000, eastern).replace(hour=9, minute=30)
                        low_end_1015 = datetime.fromtimestamp(current_day.timestamp / 1000, eastern).replace(hour=10, minute=15)
                        low_end_1330 = datetime.fromtimestamp(current_day.timestamp / 1000, eastern).replace(hour=13, minute=30)

                        low_data_1015 = client.get_aggs(ticker, 1, "minute", 
                                                      int(low_start_930.timestamp() * 1000), 
                                                      int(low_end_1015.timestamp() * 1000))
                        low_price_1015 = min([bar.low for bar in low_data_1015]) if low_data_1015 else None

                        low_data_1330 = client.get_aggs(ticker, 1, "minute", 
                                                      int(low_start_930.timestamp() * 1000), 
                                                      int(low_end_1330.timestamp() * 1000))
                        low_price_1330 = min([bar.low for bar in low_data_1330]) if low_data_1330 else None
                    except Exception as e:
                        print(f"  Error getting low prices for {ticker} on {current_date}: {str(e)}")
                        low_price_1015 = None
                        low_price_1330 = None

                    if premarket_volume >= min_premarket_volume and current_day.open > 1:
                        filtered_info = {
                            'Ticker': ticker,
                            'Date': current_date.strftime('%Y-%m-%d'),
                            'Previous Close': previous_day.close,
                            'Open': current_day.open,
                            'High': current_day.high,
                            'Low': current_day.low,
                            'Close': current_day.close,
                            'Premarket High 4am-9:29am': premarket_high,
                            'Premarket Volume': premarket_volume,
                            'LOW (9:30am-10:15am)': low_price_1015,
                            'LOW (9:30am-13:30pm)': low_price_1330,
                            'Calculated Market Cap': market_cap,
                        }
                        filtered_info.update(time_data)
                        
                        filtered_data.append(filtered_info)
                        print(f"  Found fulfilled requirements for {ticker} on {current_date}")
                        break  # Skip remaining days once a gap up is found

        except Exception as e:
            print(f"Unexpected error processing {ticker}: {str(e)}")
            continue

    return filtered_data


# Get tickers from NASDAQ and NYSE
us_tickers = get_us_tickers()

# Find tickers with gap up
filtered_tickers_data = Gap_up_reversal_long(client, us_tickers)

# # Process or print the gap up data
for data in filtered_tickers_data:
    print(data)

# # Convert the filtered tickers data to a pandas DataFrame
filtered_tickers_df = pd.DataFrame(filtered_tickers_data)

# Check if the Excel directory exists, if not, create it
if not os.path.exists('Excel'):
    os.makedirs('Excel')


# # Initialize counter
# counter = 1

# Generate file path with counter
# excel_file_path = f'Excel/filtered_tickers_data_{counter}.xlsx'
excel_file_path = f'Excel/GapAndGo.xlsx'

# # Increment counter and check if the file exists, if so, increase the counter and try again
# while os.path.exists(excel_file_path):
#     counter += 1
#     excel_file_path = f'Excel/filtered_tickers_data_{counter}.xlsx'

# Write the DataFrame to an Excel file
filtered_tickers_df.to_excel(excel_file_path, index=False)

print(f"Data stored in Excel file at: {excel_file_path}")




