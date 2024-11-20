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
- **Quality Checks**: Implementing quality checks to ensure data integrity, such as checking for missing or negative values, and validating the uniqueness of IDs. These checks are crucial for maintaining data quality throughout the ETL process, and was decided to drop the rows that didn't the criteria.

## DuckDB Database Schema

### Tables

1. **fact_sales**
   - **id**: Unique identifier for each sale (auto-generated).
   - **sale_id**: Identifier for the sale.
   - **product_id**: Identifier for the product.
   - **sale_date**: Date of the sale.
   - **quantity**: Quantity sold.
   - **sales_price**: Price at which the product was sold.
   - **total_sales**: Total sales amount (calculated as `quantity * sales_price`).

2. **dim_product**
   - **id**: Unique identifier for each product (auto-generated).
   - **product_id**: Identifier for the product.
   - **product_name**: Name of the product.
   - **category**: Category of the product.
   - **product_price**: Price of the product.

### View

- **vw_sales_product**
  - Combines data from `fact_sales` and `dim_product` to provide a comprehensive view of sales and product details.
## Partitioning in DuckDB

DuckDB supports partitioning through the use of indices, which can significantly improve query performance by allowing the database to quickly locate and access the relevant data. In this project, we simulate partitioning by creating an index on the `sale_date` column in the `fact_sales` table. This allows for efficient querying of sales data within specific date ranges.

### How It Works

- **Index Creation**: An index is created on the `sale_date` column in the `fact_sales` table using the following SQL command:
  ```sql
  CREATE INDEX IF NOT EXISTS idx_sale_date ON fact_sales(sale_date);
  ```

- **Query Optimization**: When a query is executed that filters data based on the `sale_date`, DuckDB uses the index to quickly locate the relevant rows, reducing the amount of data that needs to be scanned.

- **Example Query**: To retrieve sales data within a specific date range, you can use a query like:
  ```sql
  SELECT * FROM fact_sales WHERE sale_date BETWEEN '2023-01-01' AND '2023-12-31';
  ```

  This query will benefit from the index on `sale_date`, resulting in faster execution times.
