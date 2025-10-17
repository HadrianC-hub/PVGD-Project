#!/usr/bin/env python3
import pandas as pd
import subprocess
import time
import os
import sys
import random
from datetime import datetime, timedelta

def check_hdfs_ready():
    """Verificar si HDFS está listo"""
    max_attempts = 30
    for i in range(max_attempts):
        try:
            result = subprocess.run(
                ["nc", "-z", "hadoop-namenode", "8020"],
                capture_output=True,
                timeout=5
            )
            if result.returncode == 0:
                print("HDFS esta listo")
                return True
        except:
            pass
        print("Esperando HDFS... ({}/{})".format(i+1, max_attempts))
        time.sleep(5)
    return False

def setup_hdfs_directories():
    """Crear directorios necesarios en HDFS"""
    try:
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/data/input"], capture_output=True)
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/data/processed"], capture_output=True)
        subprocess.run(["hdfs", "dfs", "-chmod", "-R", "777", "/data"], capture_output=True)
        print("Directorios HDFS creados exitosamente")
    except Exception as e:
        print("Error creando directorios HDFS: {}".format(e))
