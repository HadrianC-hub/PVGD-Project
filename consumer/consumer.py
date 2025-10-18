#!/usr/bin/env python3
import time
import os
import subprocess
import sys
import traceback

# --- Configuración ---
POSTGRES_JDBC_URL = "jdbc:postgresql://postgres:5432/hive"
POSTGRES_USER = "hive"
POSTGRES_PASSWORD = "hive"
POSTGRES_TABLE = "retail_sales"

def log_message(message):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print("[SPARK-CONSUMER] [{}] {}".format(timestamp, message))
    sys.stdout.flush()

def find_spark_submit():
    possible_paths = [
        "/opt/spark/bin/spark-submit",
        "/usr/spark/bin/spark-submit", 
        "/usr/local/spark/bin/spark-submit",
        "/opt/bitnami/spark/bin/spark-submit",
        "/spark/bin/spark-submit"
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            log_message("spark-submit encontrado en: {}".format(path))
            return path
    
    try:
        result = subprocess.run(["which", "spark-submit"], capture_output=True, text=True)
        if result.returncode == 0:
            path = result.stdout.strip()
            log_message("spark-submit encontrado via which: {}".format(path))
            return path
    except:
        pass
    
    log_message("ERROR: No se pudo encontrar spark-submit")
    return None

def run_spark_processing():
    spark_submit_path = find_spark_submit()
    if not spark_submit_path:
        return False

    spark_script_content = '''# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

print("=== INICIANDO PROCESAMIENTO SPARK CON HIVE Y POSTGRESQL ===")

try:
    # CONFIGURACIÓN CORREGIDA CON HIVE
    spark = SparkSession.builder \\
        .appName("RetailDataProcessor") \\
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:8020") \\
        .config("spark.jars", "/opt/spark/jars/postgresql-42.5.0.jar") \\
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \\
        .config("spark.sql.warehouse.dir", "hdfs://hadoop-namenode:8020/user/hive/warehouse") \\
        .config("spark.hadoop.hive.metastore.warehouse.dir", "hdfs://hadoop-namenode:8020/user/hive/warehouse") \\
        .enableHiveSupport() \\
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("SPARK: Sesión Spark creada con soporte Hive")

    # Rutas
    hdfs_input_path = "hdfs://hadoop-namenode:8020/data/input/retail_batch_*.csv"
    print("SPARK: Buscando datos en: " + hdfs_input_path)

    # Leer datos
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(hdfs_input_path)
    record_count = df.count()
    
    print("SPARK: Registros encontrados: " + str(record_count))
    
    if record_count > 0:
        print("SPARK: Realizando limpieza y transformación...")
        
        # Limpieza (tu código existente)
        clean_df = df
        for column in clean_df.columns:
            new_column = column.replace(' ', '_').replace('/', '_').replace('-', '_') \\
                                .replace('(', '').replace(')', '').lower()
            if new_column != column:
                clean_df = clean_df.withColumnRenamed(column, new_column)
        
        numeric_columns = ['inventory_level', 'units_sold', 'units_ordered', 
                         'demand_forecast', 'price', 'discount', 'competitor_pricing']
        
        for col_name in numeric_columns:
            if col_name in clean_df.columns:
                clean_df = clean_df.withColumn(col_name, 
                    when(col(col_name).isNull(), 0.0).otherwise(col(col_name).cast("double")))
        
        string_columns = ['store_id', 'product_id', 'category', 'region', 
                        'weather_condition', 'seasonality']
        for col_name in string_columns:
            if col_name in clean_df.columns:
                clean_df = clean_df.withColumn(col_name, 
                    when(col(col_name).isNull(), "Unknown").otherwise(trim(col(col_name))))
        
        if 'holiday_promotion' in clean_df.columns:
            clean_df = clean_df.withColumn('holiday_promotion',
                when(col('holiday_promotion').isin(['1', 'True', 'true', 'YES', 'Yes']), 1)
                .when(col('holiday_promotion').isin(['0', 'False', 'false', 'NO', 'No']), 0)
                .otherwise(0))
        
        if 'date' in clean_df.columns:
            clean_df = clean_df.withColumn('date', 
                when(col('date').isNull(), date_format(current_date(), 'yyyy-MM-dd'))
                .otherwise(date_format(to_date(col('date'), 'yyyy-MM-dd'), 'yyyy-MM-dd')))
        else:
            clean_df = clean_df.withColumn('date', date_format(current_date(), 'yyyy-MM-dd'))

        print("SPARK: Esquema final:")
        clean_df.printSchema()
        clean_df.show(2)

        # --- ESCRITURA EN HIVE (CORREGIDA) ---
        print("SPARK: Escribiendo datos en Hive...")
        try:
            # Usar base de datos default de Hive
            spark.sql("USE default")
            
            # Crear tabla si no existe
            spark.sql("""
                CREATE TABLE IF NOT EXISTS retail_sales_raw (
                    date STRING,
                    store_id STRING,
                    product_id STRING,
                    category STRING,
                    region STRING,
                    inventory_level DOUBLE,
                    units_sold DOUBLE,
                    units_ordered DOUBLE,
                    demand_forecast DOUBLE,
                    price DOUBLE,
                    discount DOUBLE,
                    weather_condition STRING,
                    holiday_promotion INT,
                    competitor_pricing DOUBLE,
                    seasonality STRING
                ) STORED AS ORC
            """)
            
            # Escribir datos
            clean_df.write \\
                .mode("append") \\
                .insertInto("retail_sales_raw")
            
            print("SPARK: ✓ Datos escritos en Hive tabla 'retail_sales_raw'")
            
            # Verificar
            table_count = spark.sql("SELECT COUNT(*) FROM retail_sales_raw").collect()[0][0]
            print("SPARK: Total registros en Hive: {}".format(table_count))  # FIXED: sin f-string
            
        except Exception as hive_error:
            print("SPARK: ✗ Error con Hive: {}".format(str(hive_error)))  # FIXED: sin f-string
            print("SPARK: Continuando con PostgreSQL...")

        # --- ESCRITURA EN POSTGRESQL (EXISTENTE) ---
        print("SPARK: Escribiendo datos en PostgreSQL...")
        
        jdbc_url = "jdbc:postgresql://postgres:5432/hive"
        properties = {
            "user": "hive",
            "password": "hive", 
            "driver": "org.postgresql.Driver"
        }

        clean_df.write \\
            .mode("append") \\
            .option("createTableColumnTypes", 
                   "date VARCHAR(10), store_id VARCHAR(50), product_id VARCHAR(50), " +
                   "category VARCHAR(50), region VARCHAR(50), inventory_level DOUBLE PRECISION, " +
                   "units_sold DOUBLE PRECISION, units_ordered DOUBLE PRECISION, " +
                   "demand_forecast DOUBLE PRECISION, price DOUBLE PRECISION, " +
                   "discount DOUBLE PRECISION, weather_condition VARCHAR(50), " +
                   "holiday_promotion INTEGER, competitor_pricing DOUBLE PRECISION, " +
                   "seasonality VARCHAR(50)") \\
            .jdbc(url=jdbc_url, table="retail_sales", properties=properties)
        
        print("SPARK: ✓ Procesamiento completado - {} registros escritos".format(record_count))
        
        # Mover archivos procesados
        try:
            from py4j.java_gateway import java_import
            java_import(spark._jvm, 'org.apache.hadoop.fs.*')
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
            
            input_dir = spark._jvm.org.apache.hadoop.fs.Path("/data/input/retail_batch_*.csv")
            statuses = fs.globStatus(input_dir)
            
            for status in statuses:
                file_path = status.getPath()
                processed_path = spark._jvm.org.apache.hadoop.fs.Path(
                    "/data/processed/" + file_path.getName())
                fs.rename(file_path, processed_path)
                print("SPARK: Archivo movido: " + file_path.getName())
                
        except Exception as fs_e:
            print("SPARK: Advertencia - No se pudieron mover archivos: {}".format(str(fs_e)))  # FIXED
        
    else:
        print("SPARK: No hay datos nuevos para procesar.")

except Exception as e:
    print("SPARK: Error: " + str(e))
    import traceback
    traceback.print_exc()
finally:
    try:
        spark.stop()
        print("SPARK: Sesión Spark finalizada")
    except:
        pass
'''

    # Escribir y ejecutar script (código existente)
    script_path = '/consumer/spark_processing.py'
    try:
        with open(script_path, 'w', encoding='utf-8') as f:
            f.write(spark_script_content)
        log_message("Script Spark escrito: {}".format(script_path))
    except Exception as e:
        log_message("Error escribiendo script Spark: {}".format(e))
        return False
    
    # Limpiar warehouse local antes de ejecutar
    log_message("Limpiando warehouse local de Spark...")
    subprocess.run(["rm", "-rf", "/consumer/spark-warehouse"], capture_output=True)
    
    cmd = [spark_submit_path, '--master', 'spark://spark-master:7077', 
           '--driver-class-path', '/opt/spark/jars/postgresql-42.5.0.jar',
           '--jars', '/opt/spark/jars/postgresql-42.5.0.jar', script_path]
    
    log_message("Ejecutando Spark processing con Hive y PostgreSQL...")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        if result.stdout:
            for line in result.stdout.split('\n'):
                if line.strip():
                    log_message("SPARK: {}".format(line))
        
        if result.stderr:
            for line in result.stderr.split('\n'):
                if line.strip() and "WARN" not in line and "INFO" not in line:
                    log_message("SPARK-ERR: {}".format(line))
        
        if result.returncode == 0:
            log_message("Spark processing completado exitosamente")
            return True
        else:
            log_message("Spark processing falló (código: {})".format(result.returncode))
            return False
            
    except subprocess.TimeoutExpired:
        log_message("Spark processing timeout")
        return False
    except Exception as e:
        log_message("Error ejecutando Spark: {}".format(str(e)))
        return False

def main():
    log_message("INICIANDO SPARK CONSUMER (HIVe + POSTGRESQL)")
    
    spark_submit_path = find_spark_submit()
    if not spark_submit_path:
        log_message("ERROR CRÍTICO: No se puede encontrar spark-submit")
        return
    
    # Limpiar warehouse local al inicio
    log_message("Limpiando warehouse local...")
    subprocess.run(["rm", "-rf", "/consumer/spark-warehouse"], capture_output=True)
    
    log_message("Esperando inicialización de servicios (30s)...")
    time.sleep(30)
    
    processing_count = 0
    
    while True:
        try:
            log_message("--- Ciclo de procesamiento #{} ---".format(processing_count))
            
            success = run_spark_processing()
            if success:
                log_message("Procesamiento Spark exitoso - Datos en Hive y PostgreSQL")
            else:
                log_message("No hay datos nuevos o error en Spark")
            
            processing_count += 1
            log_message("Ciclo completado. Esperando 60 segundos...")
            time.sleep(60)
            
        except KeyboardInterrupt:
            log_message("Spark Consumer detenido por usuario")
            break
        except Exception as e:
            log_message("Error inesperado: {}".format(str(e)))
            log_message("Traceback: {}".format(traceback.format_exc()))
            time.sleep(60)

if __name__ == "__main__":
    main()