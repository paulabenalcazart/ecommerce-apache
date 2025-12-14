# 📊 E-commerce Streaming Analytics
Procesamiento Masivo de Datos con Kafka, Spark, HDFS y Streamlit

Este proyecto implementa una arquitectura de procesamiento en tiempo real para un sistema de e-commerce utilizando **Apache Kafka**, **Apache Spark Structured Streaming**, **HDFS** y un **dashboard en Streamlit**.

---

## 🧱 Arquitectura General

- **Kafka**: ingestion de eventos en tiempo real
- **Spark Structured Streaming**: procesamiento y agregaciones
- **HDFS**: almacenamiento historico en Parquet
- **Streamlit**: visualizacion de metricas

---

## 📁 Estructura del Proyecto

ecommerce-apache/
│
├── producers/ # Kafka producer (simulador de eventos)
├── spark/ # Spark streaming job
├── infra/ # Scripts de Kafka
├── hdfs/ # Creacion de carpetas HDFS
├── dashboard/ # Dashboard Streamlit
│ └── venv_dashboard/ # Entorno virtual del dashboard
├── venv/ # Entorno virtual del producer
└── docs/ # Diagramas


---

## ⚙️ Requisitos

- Java 11+
- Python 3
- Apache Kafka
- Apache Spark
- Hadoop (HDFS)
- pip / virtualenv

---

## ▶️ ORDEN DE EJECUCION (OBLIGATORIO)

Seguir este orden exactamente.

---

## 1️⃣ Iniciar Kafka

Desde la raiz del proyecto:

```bash
infra/start-kafka.sh

Kafka queda corriendo en localhost:9092.
2️⃣ Crear topicos de Kafka (solo una vez)

infra/create-topics.sh

Topicos creados:

    orders

    cart_events

    page_views

3️⃣ Crear estructura en HDFS (solo una vez)

hdfs/create_hdfs_dirs.sh

Estructura creada:

/ecommerce/
├── orders
├── views
├── cart
└── checkpoints

4️⃣ Crear y activar venv del Producer

Desde la raiz del proyecto:

python -m venv venv
source venv/bin/activate
pip install kafka-python

5️⃣ Ejecutar el Producer de Eventos

Con el venv activado:

python producers/producer_py.py

Este proceso envia eventos continuamente a Kafka simulando el sistema de e-commerce.

⚠️ No cerrar esta terminal
6️⃣ Ejecutar Spark Streaming Job

En otra terminal (no usar venv):

/opt/spark/bin/spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
  spark/spark_streaming_job.py

Este job:

    Consume eventos desde Kafka

    Procesa datos en tiempo real

    Muestra resultados en consola

    Escribe datos crudos y agregados en HDFS

⚠️ No cerrar esta terminal
7️⃣ (Opcional) Verificar eventos en Kafka

Para debug o verificacion:

/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic page_views

8️⃣ Verificar datos en HDFS

Despues de unos segundos:

hdfs dfs -ls /ecommerce
hdfs dfs -ls /ecommerce/sales_per_minute

Se generan archivos Parquet automaticamente por batch.
9️⃣ Crear y activar venv del Dashboard

Entrar a la carpeta del dashboard:

cd dashboard
python -m venv venv_dashboard
source venv_dashboard/bin/activate
pip install streamlit pyspark pandas

🔟 Ejecutar el Dashboard

Con el venv del dashboard activado:

streamlit run app.py

Abrir en el navegador:

http://localhost:8501

📈 Metricas Visualizadas

    Ventas por minuto

    Promedio de ticket por usuario

    Vistas por categoria

    Top productos mas vistos

Los datos se leen directamente desde HDFS en formato Parquet.
📝 Notas Importantes

    Spark Structured Streaming escribe agregaciones usando foreachBatch

    Parquet no soporta complete directamente en streaming

    El warning de batches atrasados es normal en modo local

    No borrar checkpoints mientras el job esta activo

🏁 Resultado Final

✔ Ingestion en tiempo real con Kafka
✔ Procesamiento distribuido con Spark
✔ Almacenamiento historico en HDFS
✔ Dashboard interactivo funcionando

Proyecto completo de Procesamiento Masivo de Datos.
📌 Recomendaciones

    Ejecutar siempre Kafka primero

    Ejecutar Spark antes del producer solo para pruebas

    Mantener los procesos corriendo mientras se visualiza el dashboard

    Detener todo con Ctrl + C cuando termine la practica


---