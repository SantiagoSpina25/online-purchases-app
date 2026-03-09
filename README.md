
![Gráfico proyecto](<Grafico online-purchases v3.jpg>)

## Ejecutar `spark_streaming_to_elastic.py`

```bash
docker compose run --rm spark sh -lc "
mkdir -p /tmp/.ivy2 && \
/opt/spark/bin/spark-submit \
--conf spark.jars.ivy=/tmp/.ivy2 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1 \
/opt/spark-apps/spark_streaming_to_elastic.py
"
```

Resumen rapido:
1. Levanta un contenedor temporal del servicio `spark`.
2. Crea carpeta de dependencias (`/tmp/.ivy2`).
3. Ejecuta `spark-submit`.
4. Descarga conectores de Kafka y Elasticsearch.
5. Corre el script de streaming.
6. Al terminar, elimina el contenedor (`--rm`).


Paso a paso

### docker comopse run --rm spark
- Ejecuta un contenedor nuevo basado en spark
- Cuando el contenedor termina se elimina automaticamente

### sh -lc "..."
- Ejecuta el shell
- Carga variables de entorno y ejecuta el comando que va entre comillas

### mkdir -p /tmp/.ivy2
- Crea un directorio temporal dentro del contenedor para guardar las dependencias

### spark-submit
- Comando oficial de Spark para:
    - Enviar un Job
    - Ejecutar Scripts PySpark

### --conf spark.jars.ivy=/tmp/.ivy2
- Configura donde Spark descarga las dependencias externas

### --packages ...
- Descarga automaticamente las librerias de Kafka y Elasticsearch

### /opt/spark-apps/spark_streaming_to_elastic.py
- Este es el script dentro del contenedor



## Verificacion de heartbeat Spark -> Airflow (paso a paso)

Este flujo sirve para que Airflow detecte si el proceso de Spark Streaming sigue vivo.

### 1. Spark escribe el heartbeat en cada micro-batch

En [`spark_streaming_to_elastic.py`]:

1. Se define `HEARTBEAT_PATH = Path("/opt/spark-logs/stream_heartbeat")`.
2. La funcion `write_heartbeat()` crea/actualiza ese archivo (`touch`).
3. Dentro de `process_batch(...)`, lo primero que se ejecuta es `write_heartbeat()`.

Resultado: cada vez que Spark procesa un batch, se refresca la fecha de modificacion del archivo.

### 2. Ese archivo se comparte por volumen con Airflow

En [`docker-compose.yml`]:

1. `spark` monta `./logs:/opt/spark-logs`.
2. `airflow-webserver` y `airflow-scheduler` montan `./logs:/opt/airflow/logs`.

Ambos apuntan a la misma carpeta del host (`./logs`), por eso:
- Spark escribe en `/opt/spark-logs/stream_heartbeat`.
- Airflow lo ve como `/opt/airflow/logs/stream_heartbeat`.

### 3. Airflow define el comando de healthcheck

En variables de entorno del compose (`STREAM_HEALTHCHECK_CMD`):

- En `airflow-scheduler` y  `airflow-webserver`:
  - `test -f /opt/airflow/logs/stream_heartbeat && find /opt/airflow/logs/stream_heartbeat -mmin -5 | grep -q stream_heartbeat`

Que valida ese comando:
1. `test -f ...` confirma que el archivo exista.
2. `find ... -mmin -5` exige que se haya actualizado en los ultimos 5 minutos.
3. `grep -q` verifica salida no vacia.

Si cualquiera falla, el comando devuelve exit code distinto de 0.

### 4. El DAG ejecuta ese comando y decide OK/FAIL

En [`dags/stream_healthcheck_dag.py`]

1. `check_stream_process()` llama `subprocess.run(HEALTHCHECK_CMD, shell=True, ...)`.
2. Si `returncode != 0`, lanza `AirflowException`.
3. El task `healthcheck_stream` (PythonOperator) queda en `failed`.

### 5. Programacion del control

El DAG `stream_healthcheck` corre cada 5 minutos (`schedule="*/5 * * * *"`).

Interpretacion:
- Si Spark sigue procesando batches, el heartbeat se refresca y el DAG pasa.
- Si Spark se cuelga o deja de consumir, el heartbeat envejece y el DAG falla.

### 6. Como diagnosticar cuando falla

Checklist rapido:
1. Verificar que Spark siga corriendo y procesando.
2. Verificar que exista `./logs/stream_heartbeat` en el host.
3. Revisar fecha de modificacion del archivo.
4. Ejecutar manualmente el comando `STREAM_HEALTHCHECK_CMD` dentro del contenedor de Airflow para reproducir el fallo.
