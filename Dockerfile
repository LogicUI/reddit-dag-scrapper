FROM apache/airflow:2.7.3

# Copy the requirements.txt into the image
COPY --chown=airflow:root requirements.txt /requirements.txt

USER airflow
# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt
