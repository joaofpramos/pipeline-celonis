## Overview

This project implements an ETL pipeline using Python, DuckDB, and Parquet files. The pipeline consists of three stages: Bronze (ingestion), Silver (transformation), and Gold (loading). The project is containerized using Docker and includes a scheduler to automate the ETL process.

## How to Run the Code

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/your-username/your-repo-name.git
   cd your-repo-name
   ```

2. **Build the Docker Image**:
   ```bash
   docker build -t etl-pipeline .
   ```

3. **Run the Docker Container**:
   ```bash
   docker run -d etl-pipeline
   ```

   This will start the ETL pipeline, which runs every few minutes as scheduled.

## Assumptions Made

- The input data is in JSON format and is placed in the `data/input` directory.
- The configuration files are correctly set up in the `configs` directory.
- The necessary Python dependencies are listed in `requirements.txt`.

## Project Structure

- **Dockerfile**: Defines the Docker image setup.
- **scheduler.py**: Manages the scheduling of ETL tasks.
- **jobs/**: Contains scripts for each ETL stage:
  - `bronze/a101_ingestion_sales_product.py`: Ingests data into the Bronze layer.
  - `silver/b201_transform_sales_product.py`: Transforms data into the Silver layer.
  - `gold/c301_load_sales_product.py`: Loads data into the Gold layer using DuckDB.
- **utils/**: Utility scripts and configurations.
- **configs/**: YAML configuration files for each ETL stage.
- **requirements.txt**: Lists Python dependencies.

## Challenges Faced

- **Data Consistency**: Ensuring data consistency across different stages of the ETL pipeline.
- **File Archiving**: Managing file archiving with consistent timestamping to avoid duplication.
- **Dockerization**: Setting up the Docker environment to ensure all dependencies are correctly installed and paths are correctly set.
- **Duplication of `ingestion_timestamp`**: When joining sales and product data, both datasets contained an `ingestion_timestamp` column. This required careful handling to avoid duplication and ensure the correct timestamp was retained.
- **Total Price Calculation**: The sales data already included a `price` column, which needed to be renamed to `sales_price` to avoid conflicts. The `total_sales` was then calculated using the `quantity` and `product_price` from the product data.
- **Quality Checks**: Implementing quality checks to ensure data integrity, such as checking for missing or negative values, and validating the uniqueness of IDs. These checks are crucial for maintaining data quality throughout the ETL process, and was decided to drop the rows that didn't meet the criteria.

## DuckDB Database Schema

### Tables

1. **Staging Table**
   - **Name**: `staging_sales_product`
   - **Format**:
     - `id` (INTEGER): Unique identifier for each record, ensuring continuity even with existing data.
     - `sale_id` (INTEGER): Unique identifier for each sale.
     - `product_id` (VARCHAR): Identifier for the product.
     - `sale_date` (DATE): Date of the sale.
     - `quantity` (INTEGER): Quantity sold.
     - `sales_price` (FLOAT): Price at which the product was sold.
     - `total_sales` (FLOAT): Calculated total sales amount (quantity * sales_price).
     - `ingestion_timestamp` (TIMESTAMP): Timestamp when the data was ingested.

2. **Monthly Partitioned Tables**
   - **Naming Convention**: `sales_<year>_<month>`
   - **Format**: Same as the staging table, including the `id` column.

### Views

1. **Yearly Sales View**
   - **Name**: `vw_sales_2024`
   - **Purpose**: Consolidates all sales data from the year 2024 into a single view, enabling easy access and analysis of the entire year's data.

### How to Query the DuckDB Database

To query the DuckDB database, you can use the `utils/query_duckdb.py` script. Here are some example queries:

1. **Show Schema**:
   ```sql
   PRAGMA show_tables;
   ```

2. **List All Tables**:
   ```sql
   SELECT table_name FROM information_schema.tables WHERE table_schema='main';
   ```

3. **Query Data from a Specific Table**:
   ```sql
   SELECT * FROM sales_2024_01 LIMIT 5;
   ```

4. **Query Data from the Yearly Sales View**:
   ```sql
   SELECT * FROM vw_sales_2024 LIMIT 5;
   ```

