import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from nsepython import *
from datetime import datetime, time
import schedule
import time as sleep_time  # To avoid name conflict with datetime.time

def fetch_oi_data(stock):
    try:
        oi_data, ltp, crontime = oi_chain_builder(stock, "latest", "full")
        
        if oi_data is None or oi_data.empty:
            return f"No OI data available for {stock}"
        
        return (stock, oi_data, ltp, crontime)
    except KeyError as ke:
        return f"KeyError for {stock}: {ke}"
    except ValueError as ve:
        return f"ValueError for {stock}: {ve}"
    except Exception as e:
        return f"Error fetching data for {stock}: {e}"

def save_to_csv(data):
    current_date = datetime.now().strftime("%d-%m-%Y")
    filename = f"oi_data_{current_date}.csv"
    
    rows = []
    for stock, oi_data, ltp, crontime in data:
        for index, row in oi_data.iterrows():
            rows.append({
                "Stock": stock,
                "LTP": ltp,
                "CronTime": crontime,
                **row.to_dict()
            })
    
    df = pd.DataFrame(rows)
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")

def fetch_all_oi_data_concurrently():
    results = []
    with ThreadPoolExecutor() as executor:
        future_to_stock = {executor.submit(fetch_oi_data, stock): stock for stock in fnolist()}
        
        for future in as_completed(future_to_stock):
            result = future.result()
            if isinstance(result, tuple):
                stock, oi_data, ltp, crontime = result
                print(f"Stock: {stock}")
                print("OI Data:", oi_data)
                print("LTP:", ltp)
                print("Cron Time:", crontime)
                print("-" * 50)
                results.append((stock, oi_data, ltp, crontime))
            else:
                print(result)
    
    if results:
        save_to_csv(results)

def job():
    current_time = datetime.now().time()
    if time(9, 15) <= current_time <= time(15, 40):
        fetch_all_oi_data_concurrently()
    else:
        print("Current time is outside the allowed range. Skipping job.")

# Schedule the job every 5 minutes
schedule.every(5).minutes.do(job)

print("Scheduler is running...")
while True:
    schedule.run_pending()
    sleep_time.sleep(1)
