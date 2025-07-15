FROM apache/airflow:3.0.2-python3.10

# Copy your requirements file
COPY requirements.txt /requirements.txt

# Upgrade pip first
RUN pip install --upgrade pip

# Install requirements (no --user needed)
RUN pip install --no-cache-dir -r /requirements.txt 