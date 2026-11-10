# "app" image: Python 3.11 + Java + Spark 3.5 (from the official Jupyter PySpark image)
# plus the project's extra libraries. Lets you run the whole project with only Docker installed.
FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3

USER root
WORKDIR /project
RUN pip install --no-cache-dir faker pymongo happybase pyarrow reportlab pillow
ENV PYTHONUNBUFFERED=1 \
    MONGO_URI=mongodb://mongodb:27017 \
    HBASE_HOST=hbase \
    HBASE_PORT=9090 \
    SPARK_DRIVER_MEMORY=3g
CMD ["python", "run_all.py", "--help"]
