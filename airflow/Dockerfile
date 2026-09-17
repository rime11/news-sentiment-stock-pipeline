# Use the official Apache Airflow base image
FROM apache/airflow:2.9.2

#Copy local requirements file into the container
COPY requirements-airflow.txt /requirements.txt

# Install any dependencies specified in the requirements file
RUN pip install --no-cache-dir -r /requirements.txt
