import time
import subprocess
import schedule
from utils.logger import get_logger

# Initialize the logger
logger = get_logger("scheduler")

def run_data_generator():
    """Run the data generation script."""
    logger.info("Running data generation script...")
    try:
        subprocess.run(["python3", "./utils/data_generator.py"], check=True)
        logger.info("Data generation completed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Data generation script failed: {e}")

def run_etl_pipeline():
    """Run the ETL pipeline: bronze, silver, and gold."""
    try:
        logger.info("Running data ingestion script...")
        subprocess.run(["python3", "./jobs/bronze/a101_ingestion_sales_product.py"], check=True)
        logger.info("a101_ingestion_sales_product completed successfully.")

        logger.info("Running silver transformation script...")
        subprocess.run(["python3", "./jobs/silver/b201_transform_sales_product.py"], check=True)
        logger.info("b201_transform_sales_product completed successfully.")

        logger.info("Running gold loading script...")
        subprocess.run(["python3", "./jobs/gold/c301_load_sales_product.py"], check=True)
        logger.info("c301_load_sales_product completed successfully.")

    except subprocess.CalledProcessError as e:
        logger.error(f"ETL pipeline script failed: {e}")

def schedule_tasks():
    """Schedule tasks to run at specified intervals."""
    # Schedule the data generator to run every 5 minutes
    schedule.every(1).minutes.do(run_data_generator)

    # Schedule the ETL pipeline to run every 5 minutes
    schedule.every(2).minutes.do(run_etl_pipeline)

    logger.info("Scheduler started. Tasks scheduled to run every 5 minutes.")

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    schedule_tasks()

