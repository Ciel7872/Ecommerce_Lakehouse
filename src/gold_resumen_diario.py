import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, datediff, round

#Validamos fecha ingreso manual
if len(sys.argv) < 2:
    print("Error: Debes proporcionar una fecha. Uso: python gold_resumen_diario.py YYYY-MM-DD")
    sys.exit(1)

execution_date = sys.argv[1]
#Iniciar agregaciones para gold
spark = SparkSession.builder \
    .appName(f"Gold_Orders_Summary_{execution_date}") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

#ruta_silver = "/home/jovyan/work/data/lakehouse/silver/orders"
#ruta_gold = "/home/jovyan/work/data/lakehouse/gold/daily_orders_summary"
# Rutas actualizadas para la capa final
ruta_silver = "/opt/airflow/project/data/lakehouse/silver/orders"
ruta_gold   = "/opt/airflow/project/data/lakehouse/gold/daily_orders_summary"
# leer silver
try:
    df_silver = spark.read.format("delta").load(ruta_silver) \
        .filter(col("ingestion_date") == execution_date)
except Exception as e:
    print(f"Error al leer Silver para la fecha {execution_date}. ¿Seguro que corriste la capa anterior?")
    sys.exit(1)

if df_silver.isEmpty():
    print("No hay datos en Silver para procesar en Gold.")
    sys.exit(0)

# Transformación GOLD: Agregaciones de Negocio
print("Calculando métricas de negocio diarias...")
df_gold = df_silver.groupBy("ingestion_date", "order_status") \
    .agg(
        count("order_id").alias("total_orders"),
        # Calculamos los días que tardó la entrega y sacamos el promedio
        round(avg(datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))), 2).alias("avg_delivery_days")
    )

#df_gold.show() #resultado en la terminal

#Guardar
df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", f"ingestion_date = '{execution_date}'") \
    .partitionBy("ingestion_date") \
    .save(ruta_gold)

