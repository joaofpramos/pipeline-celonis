FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Install necessary packages (if any)
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Create the required directory structure
RUN mkdir -p /app/logs \
&& mkdir -p /app/data/input \
&& mkdir -p /app/data/input/archive \
&& mkdir -p /app/data/bronze \
&& mkdir -p /app/data/bronze/archive \
&& mkdir -p /app/data/silver \
&& mkdir -p /app/data/silver/archive \
&& mkdir -p /app/data/gold \
&& mkdir -p /app/utils \
&& mkdir -p /app/jobs/bronze \
&& mkdir -p /app/jobs/silver \
&& mkdir -p /app/jobs/gold \
&& mkdir -p /app/configs/bronze \
&& mkdir -p /app/configs/silver \
&& mkdir -p /app/configs/gold 

# Copy the Python scripts into the container
COPY ./utils/logger.py /app/utils
COPY ./utils/data_generator.py /app/utils
COPY ./utils/__init__.py /app/utils

# Copy configs files into container
COPY ./configs/bronze/a101_ingestion_sales_product.yml /app/configs/bronze
COPY ./configs/silver/b201_transform_sales_product.yml /app/configs/silver
COPY ./configs/gold/c301_load_sales_product.yml /app/configs/gold

# Copy Python scripts and scheduler script
COPY ./jobs/bronze/a101_ingestion_sales_product.py /app/jobs/bronze
COPY ./jobs/silver/b201_transform_sales_product.py /app/jobs/silver
COPY ./jobs/gold/c301_load_sales_product.py /app/jobs/gold
COPY ./scheduler.py /app/
# Install Python dependencies (if applicable)
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app

# Run the scheduler
CMD ["python3", "/app/scheduler.py"]
