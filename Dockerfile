FROM apache/airflow:2.7.1-python3.11

# Cambiamos a root temporalmente para instalar Java (Requisito obligatorio de Spark)
USER root
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean

# Volvemos al usuario seguro de airflow e instalamos PySpark y Dotenv
USER airflow
RUN pip install pyspark==3.5.0 python-dotenv

