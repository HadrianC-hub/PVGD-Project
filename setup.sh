#!/bin/bash
set -e

echo "=== CLUSTER HADOOP + SPARK - ARQUITECTURA SIMPLIFICADA SIN POSTGRES ==="

# Limpiar servicios previos
echo "Limpiando servicios previos..."
docker-compose down

# Crear directorios necesarios
mkdir -p config
mkdir -p dataset
mkdir -p producer
mkdir -p consumer
mkdir -p streamlit

# Paso 1: Iniciar HDFS
echo "1. Iniciando HDFS..."
docker-compose up -d hadoop-namenode hadoop-datanode-1 hadoop-datanode-2
echo "Esperando 10 segundos para inicialización de HDFS..."
sleep 10

# Verificar HDFS
echo "Verificando HDFS..."
docker exec hadoop-namenode hdfs dfsadmin -report

# Paso 2: Iniciar YARN
echo "2. Iniciando YARN..."
docker-compose up -d hadoop-resourcemanager hadoop-nodemanager
sleep 5

# Paso 3: CONFIGURAR DIRECTORIOS HDFS
echo "3. Configurando directorios HDFS..."

# Forzar la salida del safe mode si está activado
echo "   • Verificando modo seguro de HDFS..."
docker exec hadoop-namenode hdfs dfsadmin -safemode leave 2>/dev/null || echo "   - Safe mode ya desactivado"

# Limpiar directorios existentes
echo "   • Limpiando directorios existentes..."
docker exec hadoop-namenode hdfs dfs -rm -r -f /data/* 2>/dev/null || echo "   - No había archivos en /data"
docker exec hadoop-namenode hdfs dfs -rm -r -f /data 2>/dev/null || echo "   - No existía directorio /data"
docker exec hadoop-namenode hdfs dfs -rm -r -f /user/hive/warehouse 2>/dev/null || echo "   - No existía warehouse"

# Esperar a que se completen las eliminaciones
sleep 3

# Crear estructura de directorios limpia
echo "   • Creando estructura de directorios..."
docker exec hadoop-namenode hdfs dfs -mkdir -p /tmp
docker exec hadoop-namenode hdfs dfs -mkdir -p /user/hive/warehouse
docker exec hadoop-namenode hdfs dfs -mkdir -p /data/input
docker exec hadoop-namenode hdfs dfs -mkdir -p /data/processed

# Aplicar permisos
echo "   • Aplicando permisos..."
docker exec hadoop-namenode hdfs dfs -chmod -R 777 /tmp
docker exec hadoop-namenode hdfs dfs -chmod -R 777 /user
docker exec hadoop-namenode hdfs dfs -chmod -R 777 /data

# Verificar estructura creada
echo "   • Verificando estructura HDFS..."
docker exec hadoop-namenode hdfs dfs -ls -R / 2>/dev/null | grep -E "(/data|/user)" || echo "   - Estructura básica creada"

# Paso 4: Iniciar Spark
echo "4. Iniciando Spark..."
docker-compose up -d spark-master spark-worker-1 spark-worker-2
sleep 5

# Paso 5: INICIALIZACIÓN CORREGIDA DE HIVE METASTORE
echo "5. Inicializando Hive Metastore..."

# Iniciar el contenedor de metastore (ahora con las variables corregidas)
docker-compose up -d hive-metastore
echo "Esperando 5 segundos para Hive Metastore..."
sleep 5
# Verificar que el contenedor está corriendo
if ! docker ps | grep -q hive-metastore; then
  echo "✗ Error: Contenedor hive-metastore no está corriendo"
  docker logs hive-metastore --tail 200
  exit 1
fi

# Esperar a que Hive Metastore arranque correctamente (comprobación robusta)
echo "Esperando a que Hive Metastore arranque (comprobación de puerto/proceso)..."
# Más intentos y sleep para entornos lentos
TRIES=60
SLEEP=3
for i in $(seq 1 $TRIES); do
    # Intentar con ss/netstat si están presentes en la imagen
    if docker exec hive-metastore sh -c "(command -v ss >/dev/null 2>&1 && ss -ltn | grep -q ':9083') || (command -v netstat >/dev/null 2>&1 && netstat -tuln | grep -q ':9083')" >/dev/null 2>&1; then
        echo "✓ Hive Metastore escuchando en el puerto 9083"
        started=true
        break
    fi

    # Fallback: si no hay herramientas de red, comprobar proceso Java del metastore
    if docker exec hive-metastore sh -c "ps aux | grep -E 'org.apache.hadoop.hive.metastore.HiveMetaStore' | grep -v grep >/dev/null 2>&1" >/dev/null 2>&1; then
        echo "✓ Proceso Metastore detectado dentro del contenedor (esperando arranque final)..."
        started=true
        break
    fi

    echo "   - esperando... ($i/$TRIES)"
    sleep $SLEEP
done
if [ "${started}" != "true" ]; then
    echo "✗ Hive Metastore no respondió en el puerto 9083 dentro del tiempo esperado. Mostrando últimos logs:"
    docker logs hive-metastore --tail 300
    exit 1
fi

echo "Esperando 15 segundos adicionales..."
sleep 15

# Verificar metastore
echo "Verificando Hive Metastore..."
if docker ps | grep -q hive-metastore; then
    echo "✓ El contenedor Hive Metastore está corriendo"
    
    # Verificación basada en logs (más robusta que netstat)
    if docker logs hive-metastore 2>&1 | grep -q "Started the new metaserver on port \[9083\]"; then
        echo "✓ Hive Metastore confirmó arranque en puerto 9083 (Logs)"
    else
        # Fallback: Si no está en logs aún, confiamos en que el proceso Java existe (ya verificado arriba)
        echo "⚠️  No se detectó mensaje de puerto en logs, pero el proceso Java existe. Asumiendo éxito."
    fi
else
    echo "✗ Hive Metastore no está corriendo"
    exit 1
fi

# Paso 6: CREAR TABLAS EN HIVE USANDO SPARK - CORREGIDO
echo "6. Creando tablas en Hive usando Spark..."

# Crear script SQL para crear tablas CON TIPOS CORREGIDOS
cat > ./create_tables.sql << 'EOF'
CREATE DATABASE IF NOT EXISTS default;
USE default;

-- Eliminar tablas existentes si hay problemas de esquema
DROP TABLE IF EXISTS retail_sales_raw;
DROP TABLE IF EXISTS retail_sales_analytics;

-- Tabla para datos brutos en formato Parquet CON TIPOS CORREGIDOS
CREATE TABLE retail_sales_raw (
    date STRING,
    store_id STRING,
    product_id STRING,
    category STRING,
    region STRING,
    inventory_level BIGINT,  -- CAMBIADO de DOUBLE a BIGINT
    units_sold BIGINT,       -- CAMBIADO de DOUBLE a BIGINT  
    units_ordered BIGINT,    -- CAMBIADO de DOUBLE a BIGINT
    demand_forecast DOUBLE,
    price DOUBLE,
    discount DOUBLE,
    weather_condition STRING,
    holiday_promotion BIGINT, -- CAMBIADO de INT a BIGINT
    competitor_pricing DOUBLE,
    seasonality STRING
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/retail_sales_raw'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'auto.purge'='true'
);

-- Tabla para análisis
CREATE TABLE retail_sales_analytics (
    date STRING,
    store_id STRING,
    product_id STRING,
    category STRING,
    region STRING,
    inventory_level BIGINT,
    units_sold BIGINT,
    units_ordered BIGINT,
    demand_forecast DOUBLE,
    price DOUBLE,
    discount DOUBLE,
    weather_condition STRING,
    holiday_promotion BIGINT,
    competitor_pricing DOUBLE,
    seasonality STRING,
    revenue DOUBLE,
    discount_amount DOUBLE
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/retail_sales_analytics'
TBLPROPERTIES (
  'parquet.compression'='SNAPPY',
  'auto.purge'='true'
);

SHOW TABLES;
DESCRIBE retail_sales_raw;
EOF

echo "Copiando create_tables.sql al contenedor spark-master..."
docker cp ./create_tables.sql spark-master:/tmp/create_tables.sql
sleep 2

# Ejecutar usando Spark SQL
echo "Ejecutando script de creación de tablas via Spark..."
docker exec spark-master /spark/bin/spark-sql \
    --master spark://spark-master:7077 \
    --name "Table-Creation" \
    -f /tmp/create_tables.sql

if [ $? -eq 0 ]; then
    echo "✅ Tablas de Hive creadas exitosamente via Spark"
else
    echo "⚠️  Reintentando creación de tablas..."
    # Fallback: crear tablas de forma más simple
    docker exec spark-master /spark/bin/spark-sql \
        --master spark://spark-master:7077 \
        -e "CREATE DATABASE IF NOT EXISTS default; USE default; DROP TABLE IF EXISTS retail_sales_raw; CREATE TABLE retail_sales_raw (date STRING, store_id STRING, product_id STRING, category STRING, region STRING, inventory_level BIGINT, units_sold BIGINT, units_ordered BIGINT, demand_forecast DOUBLE, price DOUBLE, discount DOUBLE, weather_condition STRING, holiday_promotion BIGINT, competitor_pricing DOUBLE, seasonality STRING) STORED AS PARQUET; SHOW TABLES;"
fi

# Eliminar el script temporal local
rm -f ./create_tables.sql

# Paso 7: Iniciar Data Producer
echo "7. Iniciando Data Producer..."
docker-compose up -d data-producer
sleep 5

# Verificar Data Producer
if docker ps | grep -q data-producer; then
    echo "✅ Data Producer está corriendo"
else
    echo "❌ Data Producer no está corriendo"
fi

# Paso 8: Iniciar Spark Consumer (MODIFICADO: sin PostgreSQL)
echo "8. Iniciando Spark Consumer..."
docker-compose up -d spark-consumer
echo "Esperando 10 segundos para inicialización..."
sleep 10

# Verificar Spark Consumer
if docker ps | grep -q spark-consumer; then
    echo "✅ Spark Consumer está corriendo"
    echo "   • Mostrando logs iniciales:"
    docker logs spark-consumer --tail 3
else
    echo "❌ Spark Consumer no está corriendo"
fi

# Paso 9: Iniciar Streamlit App (MODIFICADO: sin dependencias de PostgreSQL/HiveServer)
echo "9. Iniciando Streamlit App..."
docker-compose up -d streamlit-app
sleep 5

# Verificar Streamlit App
if docker ps | grep -q streamlit-app; then
    echo "✅ Streamlit App está corriendo"
else
    echo "❌ Streamlit App no está corriendo"
fi

# Pruebas finales - CORREGIDAS
echo "10. Realizando pruebas finales..."
echo ""
echo "=== ESTADO DE TODOS LOS SERVICIOS ==="
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo ""
echo "=== PRUEBAS DEL SISTEMA ==="

# Probar HDFS
echo "Probando HDFS..."
if docker exec hadoop-namenode hdfs dfsadmin -report 2>/dev/null | grep -q "Live datanodes"; then
    echo "✅ HDFS: Funcionando correctamente"
else
    echo "⚠️  HDFS: Puede tener problemas"
fi

# Probar Spark de forma más confiable
echo "Probando Spark..."
if docker ps | grep -q spark-master; then
    # Verificar que Spark Master esté respondiendo
    if curl -s http://localhost:8080 > /dev/null 2>&1; then
        echo "✅ Spark: Master UI accesible"
    else
        echo "⚠️  Spark: Master UI no accesible, pero contenedor corriendo"
    fi
else
    echo "❌ Spark: No detectado"
fi

# Verificar datos en HDFS
echo "Verificando datos en HDFS..."
echo "   • Directorio /data:"
docker exec hadoop-namenode hdfs dfs -ls -R /data/ 2>/dev/null | head -10 || echo "      - Vacío o no accesible"

echo "   • Directorio /user/hive/warehouse:"
docker exec hadoop-namenode hdfs dfs -ls -R /user/hive/warehouse/ 2>/dev/null | head -5 || echo "      - Vacío"

# Probar tablas Hive
echo "Verificando tablas Hive..."
if docker exec spark-master /spark/bin/spark-sql --master spark://spark-master:7077 -e "SHOW TABLES;" 2>/dev/null; then
    echo "✅ Tablas Hive accesibles"
else
    echo "⚠️  Tablas Hive no accesibles temporalmente"
fi

echo ""
echo "=== URLs DE ACCESO ==="
echo "📊 HDFS NameNode: http://localhost:9870"
echo "⚡ YARN ResourceManager: http://localhost:8088" 
echo "🚀 Spark Master: http://localhost:8080"
echo "📈 Streamlit App: http://localhost:8501"

echo ""
echo "=== COMANDOS ÚTILES ==="
echo "Ver todos los logs: docker-compose logs -f"
echo "Ver logs Data Producer: docker logs data-producer -f"
echo "Ver logs Spark Consumer: docker logs spark-consumer -f"
echo "Ver logs Streamlit: docker logs streamlit-app -f"
echo "Probar HDFS: docker exec hadoop-namenode hdfs dfs -ls /"
echo "Limpiar datos rápidamente: ./clean-data.sh"

echo ""
echo "=== ESTRUCTURA DEL SISTEMA ==="
echo "✅ HDFS + Parquet - Almacenamiento principal"
echo "✅ Spark - Procesamiento ETL y consultas"
echo "✅ Hive Metastore - Esquemas solamente (Derby embebido)"
echo "✅ Streamlit - Visualización desde Parquet"

echo ""
echo "=== VERIFICACIÓN FINAL HDFS ==="
echo "Espacio en HDFS:"
docker exec hadoop-namenode hdfs dfs -df -h / 2>/dev/null || echo "HDFS no disponible"

echo ""
echo "📊 Streamlit leerá datos directamente desde Parquet en HDFS via Spark"