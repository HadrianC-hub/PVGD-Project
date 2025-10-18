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
