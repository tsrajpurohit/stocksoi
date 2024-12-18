import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from nsepython import *
from datetime import datetime  # Import for date handling

def fetch_oi_data(stock):
    try:
        oi_data, ltp, crontime = oi_chain_builder(stock, "latest", "full")
        
        # Ensure oi_data is valid before proceeding
        if oi_data is None or oi_data.empty:
            return f"No OI data available for {stock}"
        
        # Assuming oi_data is a DataFrame, or you can convert it to a DataFrame if it's not
        return (stock, oi_data, ltp, crontime)
    except KeyError as ke:
        return f"KeyError for {stock}: {ke}"
    except ValueError as ve:
        return f"ValueError for {stock}: {ve}"
    except Exception as e:
        return f"Error fetching data for {stock}: {e}"

def save_to_csv(data):
    # Get the current date in dd-mm-yyyy format
    current_date = datetime.now().strftime("%d-%m-%Y")
    filename = f"oi_data_{current_date}.csv"
    
    rows = []
    for stock, oi_data, ltp, crontime in data:
        # Flatten oi_data if it's a DataFrame or nested structure, otherwise adjust this part
        for index, row in oi_data.iterrows():
            rows.append({
                "Stock": stock,
                "LTP": ltp,
                "CronTime": crontime,
                # Add relevant columns from oi_data, assuming it's a DataFrame
                **row.to_dict()  # Assuming each row in oi_data is a Series
            })
    
    # Convert to DataFrame and save to CSV
    df = pd.DataFrame(rows)
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")

def fetch_all_oi_data_concurrently():
    results = []
    with ThreadPoolExecutor() as executor:
        # Submit all tasks
        future_to_stock = {executor.submit(fetch_oi_data, stock): stock for stock in fnolist()}
        
        for future in as_completed(future_to_stock):
            result = future.result()
            if isinstance(result, tuple):
                # Unpack the result if data was fetched successfully
                stock, oi_data, ltp, crontime = result
                print(f"Stock: {stock}")
                print("OI Data:", oi_data)
                print("LTP:", ltp)
                print("Cron Time:", crontime)
                print("-" * 50)
                # Store the result to save later
                results.append((stock, oi_data, ltp, crontime))
            else:
                # Print any errors or warnings
                print(result)
    
    # After all data is fetched, save it to a CSV
    if results:
        save_to_csv(results)

# Call the function
fetch_all_oi_data_concurrently()
