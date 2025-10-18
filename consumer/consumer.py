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
