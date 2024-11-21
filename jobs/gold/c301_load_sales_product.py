import os
import pandas as pd
import duckdb
import yaml
import shutil
from datetime import datetime
from utils.logger import get_logger

# Initialize logger
logger = get_logger("c301_load_sales_product")

def load_config(config_file):
    """Load configuration from YAML file."""
    with open(config_file, "r") as file:
        return yaml.safe_load(file)

def load_silver_data(silver_dir, file_format):
    """Load and concatenate data from the silver layer."""
    files = [os.path.join(silver_dir, f) for f in os.listdir(silver_dir) if f.endswith(file_format)]
    if not files:
        logger.error("No files found in the silver directory.")
        exit(1)
    logger.info(f"Loading {len(files)} file(s) from silver directory.")
    dataframes = [pd.read_parquet(file) for file in files]
    return pd.concat(dataframes, ignore_index=True), files

def create_and_load_staging_table(con, table_name, data):
    """Create a staging table in DuckDB and load data into it."""
    logger.info(f"Creating and loading staging table {table_name}.")
    
    # Check if the table already exists and get the max id
    try:
        max_id = con.execute(f"SELECT MAX(id) FROM {table_name}").fetchone()[0]
        if max_id is None:
            max_id = 0
    except duckdb.CatalogException:
        # Table does not exist yet
        max_id = 0

    # Add an id column starting from max_id + 1
    data['id'] = range(max_id + 1, max_id + len(data) + 1)
    
    # Create the table if it doesn't exist
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM data LIMIT 0")
    
    # Load data into the table
    con.execute(f"INSERT INTO {table_name} SELECT * FROM data")

def create_monthly_tables(con, staging_table):
    """Create separate tables for each sales month and populate them."""
    logger.info("Creating separate tables for each sales month.")
    # Ensure sale_date is cast to DATE
    con.execute(f"""
        ALTER TABLE {staging_table} 
        ALTER COLUMN sale_date 
        SET DATA TYPE DATE USING CAST(sale_date AS DATE);
    """)

    # Extract distinct year and month combinations
    year_months = con.execute(f"""
        SELECT DISTINCT EXTRACT(YEAR FROM sale_date) AS sale_year,
                        EXTRACT(MONTH FROM sale_date) AS sale_month
        FROM {staging_table}
    """).fetchall()

    for year, month in year_months:
        table_name = f"sales_{int(year)}_{int(month):02d}"
        logger.info(f"Creating and populating table {table_name}.")
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} AS
            SELECT * FROM {staging_table}
            WHERE EXTRACT(YEAR FROM sale_date) = {int(year)}
              AND EXTRACT(MONTH FROM sale_date) = {int(month)}
        """)

def create_or_update_view(con):
    """Create or update a view for the year 2024."""
    logger.info("Creating or updating view for the year 2024.")
    # Collect all table names for 2024
    tables_2024 = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_name LIKE 'sales_2024_%'
    """).fetchall()

    # Construct the query to join all 2024 tables
    if tables_2024:
        union_query = " UNION ALL ".join([f"SELECT * FROM {table[0]}" for table in tables_2024])

        # Create or replace the view
        con.execute(f"""
            CREATE OR REPLACE VIEW vw_sales_2024 AS
            {union_query}
        """)
        logger.info("View vw_sales_2024 created or updated successfully.")
    else:
        logger.warning("No tables found for the year 2024 to create the view.")

def archive_files(files, archive_dir, timestamp):
    """Move files to an archive directory with a timestamp appended to filenames."""
    for file in files:
        base_name = os.path.basename(file)
        new_name = f"{os.path.splitext(base_name)[0]}_{timestamp}{os.path.splitext(base_name)[1]}"
        destination = os.path.join(archive_dir, new_name)
        shutil.move(file, destination)
        logger.info(f"Archived file {file} to {destination}.")

def main():
    """Main script to load data into DuckDB."""
    # Load configuration
    config = load_config("configs/gold/c301_load_sales_product.yml")

    # Timestamp for operations
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Load data from silver layer
    silver_dir = config["directories"]["silver"]
    file_format = config["input"]["file_format"]
    data, files = load_silver_data(silver_dir, file_format)

    # Connect to DuckDB
    db_path = config["database"]["path"]
    con = duckdb.connect(database=db_path, read_only=False)

    # Load data into staging table
    staging_table = config["tables"]["staging"]
    create_and_load_staging_table(con, staging_table, data)

    # Create separate tables for each sales month
    create_monthly_tables(con, staging_table)

    # Create or update view for 2024
    create_or_update_view(con)

    # Archive processed files
    archive_dir = os.path.join(silver_dir, "archive")
    archive_files(files, archive_dir, timestamp)

    logger.info("Data loading completed successfully.")

if __name__ == "__main__":
    main()