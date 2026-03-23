from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# configurar a Airflow de como debe comportarse
default_args = {
    'owner': 'claudia_data_engineer',
    'depends_on_past': True, # Exige que el día anterior haya sido exitoso para correr el actual
    'start_date': datetime(2017, 10, 1), # Nuestro viaje en el tiempo empieza aquí
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# def DAG
with DAG(
    'ecommerce_capas_pipeline',
    default_args=default_args,
    description='Pipeline ETL Diario de Ventas a Lakehouse',
    schedule_interval='@daily',
    catchup=True, # Magia activada: procesará todos los días desde el start_date hasta hoy
    max_active_runs=1,
    tags=['portfolio', 'lakehouse', 'pyspark'],
) as dag:

    # Agregamos esta bandera: --conf spark.jars.ivy=/tmp/.ivy
    cmd_bronze = "spark-submit --conf spark.jars.ivy=/tmp/.ivy --packages org.postgresql:postgresql:42.5.4,io.delta:delta-spark_2.12:3.1.0 /opt/airflow/project/src/bronze.py {{ ds }}"
    
    cmd_silver = "spark-submit --conf spark.jars.ivy=/tmp/.ivy --packages io.delta:delta-spark_2.12:3.1.0 /opt/airflow/project/src/silver.py {{ ds }}"
    
    cmd_gold = "spark-submit --conf spark.jars.ivy=/tmp/.ivy --packages io.delta:delta-spark_2.12:3.1.0 /opt/airflow/project/src/gold_resumen_diario.py {{ ds }}"
    
    #Envolvemos los comandos en tareas de Airflow
    task_bronze = BashOperator(task_id='extract_to_bronze', bash_command=cmd_bronze)
    task_silver = BashOperator(task_id='clean_to_silver', bash_command=cmd_silver)
    task_gold = BashOperator(task_id='aggregate_to_gold', bash_command=cmd_gold)

    #Definimos el orden lógico de ejecución (Dependencias)
    task_bronze >> task_silver >> task_gold