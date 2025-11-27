#!/bin/bash

echo "🧹 Limpiando datos en HDFS..."

# Eliminar directorios de datos
docker exec hadoop-namenode hdfs dfs -rm -r -f /data 2>/dev/null
docker exec hadoop-namenode hdfs dfs -rm -r -f /processed 2>/dev/null
docker exec hadoop-namenode hdfs dfs -rm -r -f /user/hive/warehouse 2>/dev/null

# Recrear estructura básica
docker exec hadoop-namenode hdfs dfs -mkdir -p /data/input
docker exec hadoop-namenode hdfs dfs -mkdir -p /data/processed
docker exec hadoop-namenode hdfs dfs -mkdir -p /processed
docker exec hadoop-namenode hdfs dfs -mkdir -p /user/hive/warehouse

# Asignar permisos
docker exec hadoop-namenode hdfs dfs -chmod -R 777 /data
docker exec hadoop-namenode hdfs dfs -chmod -R 777 /processed
docker exec hadoop-namenode hdfs dfs -chmod -R 777 /user/hive/warehouse

echo "✅ HDFS completamente limpio y listo"
