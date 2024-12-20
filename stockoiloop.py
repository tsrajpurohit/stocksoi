import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from nsepython import *
from datetime import datetime, time
import schedule
import time as sleep_time  # To avoid name conflict with datetime.time
import os

def fetch_oi_data(stock):
    try:
        oi_data, ltp, crontime = oi_chain_builder(stock, "latest", "full")
        
        if oi_data is None or oi_data.empty:
            print(f"No OI data available for {stock}")
            return None
        
        return (stock, oi_data, ltp, crontime)
    except Exception as e:
        print(f"Error fetching data for {stock}: {e}")
        return None

def save_to_csv(data):
    try:
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
    except Exception as e:
        print(f"Error saving data to CSV: {e}")

def fetch_all_oi_data_concurrently():
    results = []
    with ThreadPoolExecutor() as executor:
        try:
            future_to_stock = {executor.submit(fetch_oi_data, stock): stock for stock in fnolist()}
            
            for future in as_completed(future_to_stock):
                result = future.result()
                if result:
                    results.append(result)
        except Exception as e:
            print(f"Error in ThreadPoolExecutor: {e}")
    
    if results:
        save_to_csv(results)
    else:
        print("No data fetched to save.")

def job():
    try:
        current_time = datetime.now().time()
        if time(9, 15) <= current_time <= time(15, 40):
            print(f"Running job at {datetime.now()}")
            fetch_all_oi_data_concurrently()
        else:
            print("Current time is outside the allowed range. Skipping job.")
    except Exception as e:
        print(f"Error in job execution: {e}")

# Schedule the job every 5 minutes
schedule.every(5).minutes.do(job)

print("Scheduler is running...")
while True:
    try:
        schedule.run_pending()
        sleep_time.sleep(1)
    except KeyboardInterrupt:
        print("Scheduler stopped by user.")
        break
    except Exception as e:
        print(f"Unexpected error in main loop: {e}")
