FROM apache/airflow:3.0.1-python3.10

USER root

# Install git (required by dbt) and upgrade pip
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch to the airflow user
USER airflow

# Upgrade pip
RUN pip install --upgrade pip

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies via pip
RUN pip install --no-cache-dir -r requirements.txt