FROM apache/airflow:2.9.3

# Copy the requirements.txt file
COPY requirements.txt .

# Install Python packages from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt