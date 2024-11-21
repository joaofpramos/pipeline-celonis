import os
import pandas as pd
import shutil
from datetime import datetime
import yaml
from utils.logger import get_logger

# Initialize logger
logger = get_logger("b201_transform_sales_product")

def load_config(config_file):
    """Load configuration from YAML file."""
    with open(config_file, "r") as file:
        return yaml.safe_load(file)

def create_directories(directories):
    """Ensure all required directories exist."""
    for directory in directories:
        os.makedirs(directory, exist_ok=True)

def read_file(file_path, file_format):
    """Read a file in the specified format."""
    if file_format == "json":
        return pd.read_json(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

def concatenate_files(file_paths, file_format):
    """Concatenate multiple files into a single DataFrame."""
    dataframes = []
    for file_path in file_paths:
        df = read_file(file_path, file_format)
        dataframes.append(df)
    return pd.concat(dataframes, ignore_index=True)

def validate_dataframe_format(data, expected_columns, dataframe_name):
    """Validate that a DataFrame has the expected columns."""
    missing_columns = set(expected_columns) - set(data.columns)
    if missing_columns:
        logger.error(f"{dataframe_name} is missing columns: {missing_columns}. Exiting.")
        exit(1)
    logger.info(f"{dataframe_name} has the expected format.")

def log_and_drop_invalid_rows(data, condition, description):
    """Log and drop rows based on a condition."""
    invalid_rows = data[condition]
    if not invalid_rows.empty:
        logger.warning(f"Dropping rows where {description}:\n{invalid_rows}")
        data = data.drop(invalid_rows.index)
    return data

def archive_files(files, archive_dir):
    """Move files to an archive directory with a timestamp appended to filenames."""
    for file in files:
        base_name = os.path.basename(file)
        new_name = f"{os.path.splitext(base_name)[0]}_{os.path.splitext(base_name)[1]}"
        destination = os.path.join(archive_dir, new_name)
        shutil.move(file, destination)
        logger.info(f"Archived file {file} to {destination}.")

def main():
    """Main transformation script."""
    # Load configuration
    config = load_config("configs/silver/b201_transform_sales_product.yml")

    # Extract directories and file patterns
    bronze_dir = config["directories"]["bronze"]
    silver_dir = config["directories"]["silver"]
    archive_dir = config["directories"]["archive"]
    sales_pattern = config["file_patterns"]["sales"]
    product_pattern = config["file_patterns"]["product"]

    # Extract validation settings
    expected_sales_columns = config["expected_columns"]["sales"]
    expected_product_columns = config["expected_columns"]["product"]
    drop_missing_columns = config["validation"]["drop_missing"]
    check_negative_columns = config["validation"]["check_negative"]

    # Output settings
    file_format = config["output"]["file_format"]
    file_name_template = config["output"]["file_name_template"]

    # Timestamp for operations
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Create necessary directories
    create_directories([silver_dir, archive_dir])

    # Read and concatenate input files
    logger.info("Reading sales data.")
    sales_files = [os.path.join(bronze_dir, f) for f in os.listdir(bronze_dir) if f.startswith(sales_pattern.split("*")[0])]
    sales_data = concatenate_files(sales_files, "json")

    logger.info("Reading product data.")
    product_files = [os.path.join(bronze_dir, f) for f in os.listdir(bronze_dir) if f.startswith(product_pattern.split("*")[0])]
    product_data = concatenate_files(product_files, "json")

    # Validate data format
    validate_dataframe_format(sales_data, expected_sales_columns, "Sales Data")
    validate_dataframe_format(product_data, expected_product_columns, "Product Data")

    # Clean sales data: Remove rows with missing values
    sales_data = log_and_drop_invalid_rows(
        sales_data, sales_data[drop_missing_columns].isnull().any(axis=1), "missing product_id or sale_date"
    )

    # Rename price columns to avoid conflicts
    sales_data.rename(columns={"price": "sales_price"}, inplace=True)
    product_data.rename(columns={"price": "product_price"}, inplace=True)

    # Drop duplicate ingestion_timestamp column from product_data
    if 'ingestion_timestamp' in product_data.columns:
        product_data.drop(columns=['ingestion_timestamp'], inplace=True)
    
    # Join sales and product data
    logger.info("Joining sales and product data.")
    merged_data = pd.merge(sales_data, product_data, on="product_id", how="left")
    logger.debug(f"Merged data columns: {merged_data.columns}")

    # Add total_sales column
    logger.info("Adding total_sales column.")
    merged_data["total_sales"] = merged_data["quantity"] * merged_data["product_price"]

    # Perform validations after adding total_sales
    # 1. Ensure no sale_id is missing
    merged_data = log_and_drop_invalid_rows(
        merged_data, merged_data['sale_id'].isnull(), "missing sale_id"
    )

    # 2. Check for negative values in quantity and sales_price
    merged_data = log_and_drop_invalid_rows(
        merged_data, (merged_data['quantity'] < 0) | (merged_data['sales_price'] < 0), "negative values in quantity or sales_price"
    )

    # 3. Verify unique product_id counts
    unique_product_ids_bronze = product_data['product_id'].nunique()
    unique_product_ids_merged = merged_data['product_id'].nunique()
    if unique_product_ids_bronze != unique_product_ids_merged:
        logger.warning(f"Mismatch in unique product_id counts: Bronze Product Data = {unique_product_ids_bronze}, Merged Data = {unique_product_ids_merged}")

    # 4. Validate total_sales calculation
    incorrect_total_sales = merged_data[merged_data["total_sales"] != merged_data["quantity"] * merged_data["product_price"]]
    if not incorrect_total_sales.empty:
        logger.warning(f"Incorrect total_sales calculation for rows:\n{incorrect_total_sales}")
        merged_data = merged_data.drop(incorrect_total_sales.index)

    # Save the transformed data to the silver layer
    output_file = os.path.join(silver_dir, file_name_template.format(timestamp=timestamp))
    logger.info(f"Saving transformed data to {output_file}")
    merged_data.to_parquet(output_file, index=False)

    # Archive processed files
    logger.info("Archiving input files.")
    archive_files(sales_files + product_files, archive_dir)

    logger.info("Transformation completed successfully.")

if __name__ == "__main__":
    main()