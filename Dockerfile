FROM apache/airflow:2.7.3
RUN pip install --no-cache-dir flask-session==0.5.0 pyarrow==10.0.1