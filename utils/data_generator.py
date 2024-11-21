import json
import os
from faker import Faker
import random
from datetime import datetime
from logger import get_logger

# Initialize logger
logger = get_logger("data_generator")

def generate_data():
    """Generate sales and product data files with timestamped filenames."""
    try:
        # Initialize Faker
        fake = Faker()

        # Directory to save files
        output_dir = "data/input"
        os.makedirs(output_dir, exist_ok=True)

        # Current timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Generate sales data
        sales_data = [
            {
                "sale_id": i,
                "product_id": random.choice(["A12", "B23", "C34", None]),
                "sale_date": str(fake.date_between(start_date="-1y", end_date="today")) if random.random() > 0.1 else None,
                "quantity": random.randint(1, 10),
                "price": round(random.uniform(10.0, 50.0), 2)
            }
            for i in range(1, 101)
        ]

        sales_file_path = os.path.join(output_dir, f"sales_data_{timestamp}.json")
        with open(sales_file_path, 'w') as f:
            json.dump(sales_data, f, indent=4)

        logger.info(f"Sales data written to: {sales_file_path}")

        # Generate product data
        product_data = [
            {"product_id": "A12", "product_name": "Widget A", "category": "Widgets", "price": 15.5},
            {"product_id": "B23", "product_name": "Widget B", "category": "Widgets", "price": 25.0},
            {"product_id": "C34", "product_name": "Gadget C", "category": "Gadgets", "price": 45.0}
        ]

        product_file_path = os.path.join(output_dir, f"product_data_{timestamp}.json")
        with open(product_file_path, 'w') as f:
            json.dump(product_data, f, indent=4)

        logger.info(f"Product data written to: {product_file_path}")

    except Exception as e:
        logger.error(f"An error occurred during data generation: {e}")
        raise

def main():
    """Main function to run the data generator."""
    logger.info("Starting data generation...")
    generate_data()
    logger.info("Data generation completed successfully.")

if __name__ == "__main__":
    main()
