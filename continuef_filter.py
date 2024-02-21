import pandas as pd
import requests
from datetime import datetime
import pytz
from datetime import datetime



# Function to fetch intraday data for a given ticker and date
def fetch_intraday_data(ticker, date, api_key):
    print(f"Fetching data for {ticker} on {date}")
    # Format the date in YYYY-MM-DD
    formatted_date = date.strftime('%Y-%m-%d')
    
    # Polygon.io endpoint for minute aggregate data
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{formatted_date}/{formatted_date}?apiKey={api_key}"
    
    response = requests.get(url)
    data = response.json().get('results', [])

    # Process data to find specific time prices
    eastern = pytz.timezone('US/Eastern')
    times = {
        '9:30AM-10:15AM': {'start': eastern.localize(datetime.combine(date, datetime.strptime('09:30', '%H:%M').time())),
                           'end': eastern.localize(datetime.combine(date, datetime.strptime('10:15', '%H:%M').time()))},
        '10:00AM': eastern.localize(datetime.combine(date, datetime.strptime('10:00', '%H:%M').time())),
        '10:30AM': eastern.localize(datetime.combine(date, datetime.strptime('10:30', '%H:%M').time())),
        '11:30AM': eastern.localize(datetime.combine(date, datetime.strptime('11:30', '%H:%M').time())),
        '12:30PM': eastern.localize(datetime.combine(date, datetime.strptime('12:30', '%H:%M').time()))
    }
    
    prices = {}
    for i, (time_key, time_value) in enumerate(times.items()):
        if time_key == '9:30AM-10:15AM':
            prices[time_key] = min([d['l'] for d in data if time_value['start'].timestamp() <= d['t'] / 1000 <= time_value['end'].timestamp()], default=None)
        else:
            price = next((d['o'] for d in data if time_value.timestamp() == d['t'] / 1000), None)
            prices[time_key] = price
        print(f"Processed {i+1} out of {len(times)} time intervals.")

    return prices

# Load Excel file
df = pd.read_excel('Excel/filtered_tickers_data_2.xlsx')

# Convert the 'Date' column to datetime objects
df['Date'] = pd.to_datetime(df['Date'])

# Polygon.io API key
api_key = 'IL6CWvjVi26OCuPNehXi47pJKBXeOtnk'

# Update DataFrame with intraday data
for index, row in df.iterrows():
    ticker = row['Ticker']
    date = row['Date']
    
    intraday_prices = fetch_intraday_data(ticker, date, api_key)
    
    for time, price in intraday_prices.items():
        df.at[index, f'Price_{time}'] = price
    print(f"Progress: {index + 1} out of {len(df)} completed.")

# Save updated DataFrame to Excel
df.to_excel('Excel/updated_filtered_tickers_data.xlsx', index=False)
