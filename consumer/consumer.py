import subprocess

def main():
    print("=== INICIANDO SPARK CONSUMER (MODO CONTINUO) ===")
    
    # Definimos el script de PySpark
    spark_script = """# -*- coding: utf-8 -*-
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from py4j.java_gateway import java_import

print(">>> INICIALIZANDO SESION SPARK PERSISTENTE...")

# 1. Configuración de Spark (Solo se hace una vez)
spark = SparkSession.builder \\
    .appName("RetailDataProcessor-Continuous") \\
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:8020") \\
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \\
    .config("spark.sql.warehouse.dir", "hdfs://hadoop-namenode:8020/user/hive/warehouse") \\
    .config("spark.sql.parquet.mergeSchema", "true") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .enableHiveSupport() \\
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print(">>> Sesion Spark iniciada y lista para escuchar.")

# Configuración de rutas
INPUT_PATH = "/data/input/"
PROCESSED_PATH_BASE = "/data/processed/"

def get_filesystem():
    # Obtener objeto FileSystem de Hadoop via JVM
    java_import(spark._jvm, 'org.apache.hadoop.fs.*')
    return spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

def process_batch():
    fs = get_filesystem()
    path_obj = spark._jvm.org.apache.hadoop.fs.Path(INPUT_PATH)
    
    # 1. Listar archivos explícitamente (Evita leer carpeta genérica)
    if not fs.exists(path_obj):
        return 0
        
    # Obtener lista de archivos Parquet recursivamente
    files_to_process = []
    
    try:
        remote_iter = fs.listFiles(path_obj, True) # True = recursivo
        while remote_iter.hasNext():
            file_status = remote_iter.next()
            path_str = file_status.getPath().toString()
            if path_str.endswith(".parquet") and "_temporary" not in path_str:
                files_to_process.append(path_str)
    except Exception as e:
        print("Error listando archivos: " + str(e))
        return 0

    if not files_to_process:
        return 0
        
    # Procesamos en lotes de máximo 20 archivos para no saturar memoria si se acumulan
    batch_files = files_to_process[:20]
    print(">>> Procesando lote de {} archivos...".format(len(batch_files)))

    try:
        # 2. Leer solo los archivos específicos detectados
        # Pasamos la lista de archivos a Spark
        df = spark.read.option("mergeSchema", "true").parquet(*batch_files)
        
        count = df.count()
        print("   -> Registros leidos: {}".format(count))
        
        if count > 0:
            # 3. Escribir a Hive (Append)
            # Normalizamos columnas por si acaso
            cols = [c.lower() for c in df.columns]
            df = df.toDF(*cols)
            
            # Aseguramos que la tabla existe
            spark.sql("CREATE TABLE IF NOT EXISTS retail_sales_raw (date STRING, store_id STRING, product_id STRING, category STRING, region STRING, inventory_level BIGINT, units_sold BIGINT, units_ordered BIGINT, demand_forecast DOUBLE, price DOUBLE, discount DOUBLE, weather_condition STRING, holiday_promotion BIGINT, competitor_pricing DOUBLE, seasonality STRING) STORED AS PARQUET LOCATION '/user/hive/warehouse/retail_sales_raw'")
            
            # Insertar
            df.write.mode("append").insertInto("retail_sales_raw")
            print("   -> Datos insertados en Hive")

        # 4. Mover solo los archivos procesados
        timestamp_dir = "batch_{}".format(int(time.time()))
        dest_dir = "{}{}".format(PROCESSED_PATH_BASE, timestamp_dir)
        dest_path_obj = spark._jvm.org.apache.hadoop.fs.Path(dest_dir)
        
        if not fs.exists(dest_path_obj):
            fs.mkdirs(dest_path_obj)
            
        for file_path_str in batch_files:
            src = spark._jvm.org.apache.hadoop.fs.Path(file_path_str)
            # Nombre archivo
            fname = src.getName()
            dst = spark._jvm.org.apache.hadoop.fs.Path("{}/{}".format(dest_dir, fname))
            
            fs.rename(src, dst)
            
        print(">>> Lote completado. Archivos movidos a " + timestamp_dir)
        return count

    except Exception as e:
        print("!!! Error procesando lote: " + str(e))
        # No movemos archivos si falló, para reintentar luego
        return 0

# BUCLE INFINITO DENTRO DE SPARK
while True:
    start = time.time()
    processed_count = process_batch()
    duration = time.time() - start
    
    if processed_count == 0:
        # Si no hay datos, dormir un poco
        time.sleep(2) 
    else:
        # Si hubo datos, procesar inmediatamente o esperar poco
        print(">>> Ciclo completado en {:.2f}s".format(duration))
        time.sleep(1)

"""
    
    # Escribir el script interno
    script_path = "/consumer/runner.py"
    with open(script_path, "w") as f:
        f.write(spark_script)
    
    print("Script generado en: " + script_path)
    
    # Ejecutar Spark Submit
    cmd = [
        "/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--driver-memory", "1g",
        "--executor-memory", "1g",
        "--conf", "spark.rpc.message.maxSize=128",
        script_path
    ]
    
    print("Ejecutando Spark Submit persistente...")
    subprocess.run(cmd)

if __name__ == "__main__":
    main()