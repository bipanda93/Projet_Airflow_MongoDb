FROM apache/airflow:2.9.0
RUN pip install --no-cache-dir mysql-connector-python
