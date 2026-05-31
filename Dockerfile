FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

RUN apt-get update && apt-get install -y wget && \
    # Driver JDBC para conectar PySpark ao PostgreSQL
    wget -q https://jdbc.postgresql.org/download/postgresql-42.7.1.jar \
         -O /usr/local/spark/jars/postgresql-42.7.1.jar

USER jovyan

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
