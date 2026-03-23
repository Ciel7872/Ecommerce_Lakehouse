import sys
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


# #Validamos fecha ingreso manual e indica formato ejemplo 2017-10-02
if len(sys.argv) < 2:
    print("Error: Debes proporcionar una fecha. Uso: python bronze_layer.py YYYY-MM-DD")
    sys.exit(1)

execution_date = sys.argv[1]
#Ejecuccion 
# Cargar variables de entorno (ruta dentro del contenedor -- sin airflow)
#load_dotenv("/home/jovyan/work/.env")
load_dotenv("/opt/airflow/project/.env")

spark = SparkSession.builder \
    .appName(f"Bronze_Orders_{execution_date}") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.4,io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# conexion del contenedor Postgres
DB_HOST = os.getenv('DB_HOST_BRONZE', 'postgres_oltp') 
jdbc_url = f"jdbc:postgresql://{DB_HOST}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

connection_properties = {
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "driver": "org.postgresql.Driver"
}

#Extraer de Postgres FILTRANDO por la fecha de exe
query = f"(SELECT * FROM orders WHERE DATE(order_purchase_timestamp) = '{execution_date}') AS daily_orders"

df_orders = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)

# validar si hay datoos si no finalizar
if df_orders.isEmpty():
    print(f"No hay órdenes nuevas para el día {execution_date}.")
    sys.exit(0)

df_orders = df_orders.withColumn("ingestion_date", lit(execution_date))

# Esta es la ruta sincronizada dentro del contenedor
#ruta_bronze = "/home/jovyan/work/data/lakehouse/bronze/orders"
# o : 
#ruta_bronze = "file:///opt/airflow/project/data/lakehouse/bronze/orders"
#Nueva ruta para airflow
ruta_bronze = "/opt/airflow/project/data/lakehouse/bronze/orders"
#guardamos con modo overwrite y no append
df_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"ingestion_date = '{execution_date}'") \
    .partitionBy("ingestion_date") \
    .save(ruta_bronze)
