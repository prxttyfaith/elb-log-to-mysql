# Use Python 3.13 slim version
FROM python:3.13-slim

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies 
RUN apt-get update && apt-get install -y \
    gcc \
 && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy everything in this folder (including your .env, .py files, etc.)
COPY . . 

# Run the ETL script when the container starts
CMD ["python", "etl_elb_log_to_mysql.py"]