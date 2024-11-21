import os
import pandas as pd

def load_parquet_files(directory, file_pattern):
    """Load and concatenate Parquet files from a directory."""
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.parquet') and file_pattern in f]
    if not files:
        print("No Parquet files found in the specified directory.")
        return pd.DataFrame()
    
    print(f"Loading {len(files)} Parquet file(s) from {directory}.")
    dataframes = [pd.read_parquet(file) for file in files]
    return pd.concat(dataframes, ignore_index=True)

def query_data(data, query):
    """Query the DataFrame using a pandas query string."""
    try:
        result = data.query(query)
        print(f"Query executed successfully. Number of rows returned: {len(result)}")
        return result
    except Exception as e:
        print(f"Failed to execute query: {e}")
        return pd.DataFrame()

def main():
    # Directory and file pattern for Parquet files
    silver_dir = "data/silver/archive"
    file_pattern = "transformed_sales_product"

    # Load Parquet files
    data = load_parquet_files(silver_dir, file_pattern)
    if data.empty:
        print("No data loaded. Exiting.")
        return

# Set pandas option to display all columns
    pd.set_option('display.max_columns', None)

    # Example 1: Query all data
    print("Querying all data.")
    all_data = data
    print("All Data:")
    print(all_data.head(), "\n")

    # Example 2: Get all sales with total_sales greater than 1000
    query = "total_sales > 100"
    print(f"Querying data with condition: {query}")
    result = query_data(data, query)
    print("Sales with total_sales > 1000:")
    print(result.head(), "\n")

    # Example 3: Get sales for a specific product_id
    specific_product_id = 3  # Replace with an actual product_id from your data
    query = f"product_id == {specific_product_id}"
    print(f"Querying data for product_id: {specific_product_id}")
    result = query_data(data, query)
    print(f"Sales for product_id {specific_product_id}:")
    print(result.head(), "\n")

    # Example 4: Get sales within a specific date range
    start_date = "2023-01-01"
    end_date = "2023-12-31"
    query = f"'{start_date}' <= sale_date <= '{end_date}'"
    print(f"Querying data for sales between {start_date} and {end_date}")
    result = query_data(data, query)
    print(f"Sales between {start_date} and {end_date}:")
    print(result.head(), "\n")

    # Example 5: Get sales with quantity greater than a threshold
    quantity_threshold = 7
    query = f"quantity > {quantity_threshold}"
    print(f"Querying data with quantity greater than {quantity_threshold}")
    result = query_data(data, query)
    print(f"Sales with quantity > {quantity_threshold}:")
    print(result.head(), "\n")

if __name__ == "__main__":
    main()