import os
import sys


# Correctly add the project root directory to sys.path
#current_file = os.path.abspath(__file__)
#project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_file)))
#sys.path.append(project_root)
import pandas as pd
import yaml
from datetime import datetime
import shutil
from utils.logger import get_logger

# Timestamp for the entire script
INGESTION_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H-%M-%S")

# Configuration file path
CONFIG_PATH = "configs/bronze/a101_ingestion_sales_product.yml"

def load_config(config_path):
    """Load the configuration file, creating it with defaults if missing."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config

def validate_file_format(file_name, expected_pattern):
    """Validate if a file matches the expected pattern."""
    return file_name.startswith(expected_pattern.split("*")[0]) and file_name.endswith(".json")

def read_file(file_path, file_format):
    """Read a file in the specified format."""
    if file_format == "json":
        return pd.read_json(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

def write_file(data, file_path, file_format):
    """Write a DataFrame to a file in the specified format."""
    if file_format == "json":
        data.to_json(file_path, orient="records", indent=4)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

def concatenate_files(file_paths, file_format):
    """Concatenate multiple files into a single DataFrame."""
    dataframes = []
    for file_path in file_paths:
        df = read_file(file_path, file_format)
        dataframes.append(df)
    return pd.concat(dataframes, ignore_index=True)

def validate_required_columns(data, required_columns, dataset_name):
    """Validate that all required columns are present in the dataset."""
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        logger.error(f"Missing columns in {dataset_name}: {missing_columns}")
        raise ValueError(f"Missing columns in {dataset_name}: {missing_columns}")
    logger.info(f"All required columns present in {dataset_name}.")

def validate_negative_values(data, columns_to_check, dataset_name):
    """Validate that no negative values exist in the specified columns."""
    for col in columns_to_check:
        if (data[col] < 0).any():
            logger.error(f"Negative values found in column '{col}' of {dataset_name}.")
            raise ValueError(f"Negative values found in column '{col}' of {dataset_name}.")
    logger.info(f"No negative values in {dataset_name}.")

def validate_unique_ids(data, unique_id_column, dataset_name):
    """Validate that all IDs in the specified column are unique."""
    if data[unique_id_column].duplicated().any():
        logger.error(f"Duplicate IDs found in column '{unique_id_column}' of {dataset_name}.")
        raise ValueError(f"Duplicate IDs found in column '{unique_id_column}' of {dataset_name}.")
    logger.info(f"All IDs are unique in column '{unique_id_column}' of {dataset_name}.")

def move_files(files, destination):
    """Move files to a new destination with a timestamp appended to filenames."""
    for file in files:
        base_name = os.path.basename(file)
        new_name = f"{os.path.splitext(base_name)[0]}_{os.path.splitext(base_name)[1]}"
        destination_path = os.path.join(destination, new_name)
        shutil.move(file, destination_path)
        logger.info(f"Moved file {file} to {destination_path}")

def main():
    global logger
    logger = get_logger("a101_ingestion_sales_product")

    # Load configurations
    config = load_config(CONFIG_PATH)
    expected_formats = config["expected_formats"]
    directories = config["directories"]
    data_format = config["data_format"]
    validation = config["validation"]

    input_dir = directories["input"]
    bronze_dir = directories["bronze"]
    archive_dir = directories["archive"]

    # Ensure directories exist
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(bronze_dir, exist_ok=True)
    os.makedirs(archive_dir, exist_ok=True)

    # Check for input files
    input_files = os.listdir(input_dir)
    sales_files = [os.path.join(input_dir, f) for f in input_files if validate_file_format(f, expected_formats["sales"])]
    product_files = [os.path.join(input_dir, f) for f in input_files if validate_file_format(f, expected_formats["product"])]

    if not sales_files or not product_files:
        logger.error("Missing required files: at least one sales and one product file must exist.")
        exit(1)

    logger.info(f"Found sales files: {sales_files}")
    logger.info(f"Found product files: {product_files}")

    # Concatenate files if there are multiple
    sales_format = data_format["input"]["sales"]
    product_format = data_format["input"]["product"]
    if len(sales_files) > 1:
        logger.info("Concatenating sales files.")
        sales_data = concatenate_files(sales_files, sales_format)
    else:
        sales_data = read_file(sales_files[0], sales_format)

    if len(product_files) > 1:
        logger.info("Concatenating product files.")
        product_data = concatenate_files(product_files, product_format)
    else:
        product_data = read_file(product_files[0], product_format)

    # Perform validations
    validate_required_columns(sales_data, validation["required_columns"]["sales"], "sales data")
    validate_required_columns(product_data, validation["required_columns"]["product"], "product data")

    # Add ingestion timestamp
    logger.info("Adding ingestion timestamp.")
    sales_data["ingestion_timestamp"] = INGESTION_TIMESTAMP
    product_data["ingestion_timestamp"] = INGESTION_TIMESTAMP

    # Save the processed files to bronze directory in JSON format
    bronze_format = data_format["output"]["bronze"]
    sales_bronze_path = os.path.join(bronze_dir, f"sales_data_bronze_{INGESTION_TIMESTAMP}.{bronze_format}")
    product_bronze_path = os.path.join(bronze_dir, f"product_data_bronze_{INGESTION_TIMESTAMP}.{bronze_format}")

    write_file(sales_data, sales_bronze_path, bronze_format)
    write_file(product_data, product_bronze_path, bronze_format)

    logger.info(f"Sales data saved to bronze: {sales_bronze_path}")
    logger.info(f"Product data saved to bronze: {product_bronze_path}")

    # Archive processed files
    logger.info("Archiving processed files.")
    move_files(sales_files + product_files, archive_dir)

    logger.info("Ingestion completed successfully.")

if __name__ == "__main__":
    main()