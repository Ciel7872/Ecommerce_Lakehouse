# 🛒 E-Commerce Lakehouse con Arquitectura Capa/Medallón

Este proyecto es una implementación completa de un Data Lakehouse diseñado para procesar y analizar datos de un e-commerce. A través de una arquitectura en capas (Bronze, Silver, Gold), el proyecto simula un entorno real de ingeniería de datos, desde la extracción transaccional hasta la visualización final.

## 📦 Sobre el Dataset (Olist)

Este proyecto utiliza el dataset público de **Olist**, la tienda por departamentos más grande de los marketplaces en Brasil. El dataset contiene información real y anonimizada de e-commerce, abarcando múltiples dimensiones del negocio:

* **Clientes:** Datos y distribución geográfica.
* **Pedidos y Artículos:** Estado del pedido, precios, fechas de compra y métricas de entrega.
* **Pagos:** Métodos de pago (tarjeta, boleto, etc.) y valor de las cuotas.
* **Productos:** Categorías de los artículos vendidos.

El objetivo de este pipeline es transformar estos datos transaccionales crudos (archivos CSV separados) en un modelo analítico unificado que permita extraer *insights* de negocio, como el volumen de ventas diario y el comportamiento de las órdenes.

## 🛠️ Stack Tecnológico

* **Docker:** Contenerización de todos los servicios para garantizar entornos aislados y reproducibles.
* **PostgreSQL:** Base de datos transaccional que actúa como sistema de origen de los datos (OLTP).
* **Apache Airflow:** Orquestación y programación de los pipelines de datos (DAGs).
* **PySpark & Delta Lake:** Motor principal de procesamiento distribuido para las transformaciones entre las capas Bronze, Silver y Gold, garantizando transacciones ACID.
* **Jupyter & Seaborn:** Entorno de análisis y visualización para el consumo de los datos curados en la capa Gold.

## 📊 Arquitectura y Flujo de Datos

`![Arquitectura del Proyecto](iamges/arquitectura.png)`

El pipeline se orquesta mediante Airflow, asegurando la ejecución secuencial y el monitoreo de las tareas.

*(Agrega aquí la captura de los cuadraditos verdes de Airflow)*
`![DAGs en Airflow](ruta/a/tu/run_airflow.png)`

Los datos fluyen a través de las siguientes capas:
1.  **Bronze:** Ingesta de datos crudos desde PostgreSQL.
2.  **Silver:** Limpieza, filtrado y estandarización de los datos.
3.  **Gold:** Agregaciones y modelos de negocio listos para el consumo (Resumen diario de ventas).

`![Estructura Delta Lake](images/capas.png)`
`![Estructura RAW](images/capa_raw.png)`

## Vizualizacion directa
`![Grafico](images/prueba_basica.png)`
`![Reporte Resumen](images/tabla_resumen.png)`


## 🚀 Próximos Pasos
El proyecto se encuentra en evolución constante. Las siguientes características están planificadas para las próximas iteraciones:

* **Ingesta Completa del Dataset:** Incorporar y limpiar en la capa Silver los archivos CSV restantes de Olist (geolocalización, reviews, vendedores, etc.) para tener el modelo de datos completo.
* **Nuevos Modelos de Negocio (Capa Gold):**
  * **Modelo RFM de Clientes:** Cálculo de *Recency, Frequency, Monetary* para segmentar a los usuarios según su valor y comportamiento de compra.
  * **Rendimiento Logístico por Código Postal:** Creación de un *data mart* geoespacial para identificar mediante mapas de calor qué zonas geográficas de Brasil sufren las mayores demoras en las entregas.
* **Integración con Power BI Desktop:** Conexión directa a la capa Gold del Lakehouse para reemplazar los notebooks por dashboards interactivos orientados a la toma de decisiones.


------

## 🚀 Cómo ejecutar el proyecto

Para levantar la infraestructura completa (Postgres, Spark, Airflow, etc.), colócate en la raíz del proyecto y utiliza Docker Compose:
Una vez que los contenedores estén corriendo, el pipeline puede ser orquestado de forma automática mediante Apache Airflow (localhost:8080).

```bash
docker-compose up -d

