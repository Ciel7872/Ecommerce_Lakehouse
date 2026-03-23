import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

#Validamos fecha ingreso manual
if len(sys.argv) < 2:
    print("Error: Debes proporcionar una fecha. Uso: python silver.py YYYY-MM-DD")
    sys.exit(1)

execution_date = sys.argv[1]
print(f"Iniciando limpieza en Capa Silver para la fecha: {execution_date}")

# solo leemos delta
spark = SparkSession.builder \
    .appName(f"Silver_Orders_{execution_date}") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Rutas internas de Docker
#ruta_bronze = "/home/jovyan/work/data/lakehouse/bronze/orders"
#ruta_silver = "/home/jovyan/work/data/lakehouse/silver/orders"
# Rutas actualizadas para que Airflow las encuentre
ruta_bronze = "/opt/airflow/project/data/lakehouse/bronze/orders"
ruta_silver = "/opt/airflow/project/data/lakehouse/silver/orders"
# leemos bronce
try:
    df_bronze = spark.read.format("delta").load(ruta_bronze) \
        .filter(col("ingestion_date") == execution_date)
except Exception as e:
    print(f"Error al leer la capa Bronze. Asegúrate de que los datos existan para {execution_date}.")
    sys.exit(1)

if df_bronze.isEmpty():
    print("No hay datos en Bronze para limpiar en esta fecha.")
    sys.exit(0)

# Transformaciones (Data Cleansing)
print("Aplicando reglas de calidad de datos...")
df_silver = df_bronze \
    .dropDuplicates(["order_id"]) \
    .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))) \
    .withColumn("order_approved_at", to_timestamp(col("order_approved_at"))) \
    .withColumn("order_delivered_carrier_date", to_timestamp(col("order_delivered_carrier_date"))) \
    .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date"))) \
    .withColumn("order_estimated_delivery_date", to_timestamp(col("order_estimated_delivery_date"))) \
    .filter(col("order_status").isNotNull()) # Descartamos filas donde el estado venga vacío por error

# Guardar en Silver usamos overwrite y dejamos de usar append
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"ingestion_date = '{execution_date}'") \
    .partitionBy("ingestion_date") \
    .save(ruta_silver)
