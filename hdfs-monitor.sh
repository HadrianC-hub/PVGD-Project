#!/bin/bash
echo "=== MONITOR HDFS - ARCHIVOS RETAIL ==="

echo ""
echo "1. ARCHIVOS EN HDFS:"
docker exec hadoop-namenode hdfs dfs -ls -h /data/input/retail_batch_*.csv

echo ""
echo "2. ÚLTIMO ARCHIVO CREADO:"
docker exec hadoop-namenode hdfs dfs -ls -t /data/input/retail_batch_*.csv | head -1

echo ""
echo "3. CONTENIDO DEL ÚLTIMO ARCHIVO (primeras 2 líneas):"
LATEST_FILE=$(docker exec hadoop-namenode hdfs dfs -ls -t /data/input/retail_batch_*.csv | head -1 | awk '{print $8}')
if [ ! -z "$LATEST_FILE" ]; then
    echo "Archivo: $LATEST_FILE"
    docker exec hadoop-namenode hdfs dfs -cat $LATEST_FILE | head -2
else
    echo "No se encontraron archivos retail_batch"
fi

echo ""
echo "4. ESTADÍSTICAS:"
echo "Total de archivos:" $(docker exec hadoop-namenode hdfs dfs -ls /data/input/retail_batch_*.csv 2>/dev/null | wc -l)
echo "Total de registros aproximado:" $(docker exec hadoop-namenode hdfs dfs -cat /data/input/retail_batch_*.csv 2>/dev/null | wc -l)

echo ""
echo "5. ESPACIO UTILIZADO:"
docker exec hadoop-namenode hdfs dfs -du -h /data/input/