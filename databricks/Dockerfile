FROM apache/airflow:2.8.1

# Set the working directory
WORKDIR /opt/airflow

# Copy the requirements.txt file into the image
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt