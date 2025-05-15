FROM apache/airflow:3.0.0

USER root

# Install Chromium and Chromedriver (ARM-compatible)
RUN apt-get update && apt-get install -y \
    chromium chromium-driver \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
