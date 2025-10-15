#!/bin/bash
set -e

echo "=== CLUSTER HADOOP + HIVE + SPARK - INICIALIZACIÓN CORREGIDA ==="

# Limpiar servicios previos
echo "Limpiando servicios previos..."
docker-compose down

# Crear directorios necesarios
mkdir -p config
mkdir -p dataset
mkdir -p producer
mkdir -p consumer
mkdir -p streamlit

# Crear configuración HDFS
echo "Creando configuraciones HDFS..."
cat > config/core-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://hadoop-namenode:8020</value>
  </property>
  <property>
    <name>hadoop.proxyuser.root.hosts</name>
    <value>*</value>
  </property>
  <property>
    <name>hadoop.proxyuser.root.groups</name>
    <value>*</value>
  </property>
</configuration>
EOF

cat > config/hdfs-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>dfs.replication</name>
    <value>1</value>
  </property>
  <property>
    <name>dfs.namenode.name.dir</name>
    <value>file:///hadoop/dfs/name</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>file:///hadoop/dfs/data</value>
  </property>
  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
</configuration>
EOF

cat > config/yarn-site.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
  <property>
    <name>yarn.resourcemanager.hostname</name>
    <value>hadoop-resourcemanager</value>
  </property>
  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
  </property>
</configuration>
EOF

cat > config/mapred-site.xml << 'EOF'
<?xml version="1.0"?>
<configuration>
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
  </property>
</configuration>
EOF

# Crear hive-site.xml optimizado
echo "Creando hive-site.xml optimizado..."
cat > config/hive-site.xml << 'EOF'
<?xml version="1.0"?>
<configuration>
  <!-- Configuración de PostgreSQL para Metastore -->
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:postgresql://postgres:5432/hive</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.postgresql.Driver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>hive</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>hive</value>
  </property>
  
  <!-- Configuración de Metastore -->
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://hive-metastore:9083</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/user/hive/warehouse</value>
  </property>
  
  <!-- Configuración de HiveServer2 -->
  <property>
    <name>hive.server2.thrift.port</name>
    <value>10000</value>
  </property>
  <property>
    <name>hive.server2.thrift.bind.host</name>
    <value>0.0.0.0</value>
  </property>
  <property>
    <name>hive.server2.authentication</name>
    <value>NONE</value>
  </property>
  
  <!-- Configuraciones para evitar problemas -->
  <property>
    <name>hive.metastore.event.db.notification.api.auth</name>
    <value>false</value>
  </property>
  <property>
    <name>hive.execution.engine</name>
    <value>mr</value>
  </property>
  
  <!-- Configuraciones básicas -->
  <property>
    <name>hive.support.concurrency</name>
    <value>false</value>
  </property>
  <property>
    <name>hive.txn.manager</name>
    <value>org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager</value>
  </property>
</configuration>
EOF

# Paso 1: Iniciar servicios base
echo "1. Iniciando PostgreSQL y HDFS..."
docker-compose up -d postgres hadoop-namenode hadoop-datanode
echo "Esperando 5 segundos para inicialización..."
sleep 5

# Verificar HDFS
echo "Verificando HDFS..."
docker exec hadoop-namenode hdfs dfsadmin -report

# Verificar PostgreSQL
echo "Verificando PostgreSQL..."
sleep 10
if docker exec postgres pg_isready -U hive -d hive; then
    echo "✓ PostgreSQL está listo"
    
    # Crear tabla si no existe
    echo "Creando tabla retail_sales en PostgreSQL..."
    docker exec postgres psql -U hive -d hive -c "
    CREATE TABLE IF NOT EXISTS retail_sales (
        date VARCHAR(10),
        store_id VARCHAR(50),
        product_id VARCHAR(50),
        category VARCHAR(50),
        region VARCHAR(50),
        inventory_level DOUBLE PRECISION,
        units_sold DOUBLE PRECISION,
        units_ordered DOUBLE PRECISION,
        demand_forecast DOUBLE PRECISION,
        price DOUBLE PRECISION,
        discount DOUBLE PRECISION,
        weather_condition VARCHAR(50),
        holiday_promotion INTEGER,
        competitor_pricing DOUBLE PRECISION,
        seasonality VARCHAR(50)
    );"
    echo "Tabla retail_sales verificada/creada en PostgreSQL"
else
    echo "✗ PostgreSQL no está respondiendo"
fi

# Paso 2: Iniciar YARN
echo "2. Iniciando YARN..."
docker-compose up -d hadoop-resourcemanager hadoop-nodemanager
sleep 5

# Paso 3: LIMPIAR Y CONFIGURAR DIRECTORIOS HDFS - CORREGIDO
echo "3. Limpiando y configurando directorios HDFS..."

# Forzar la salida del safe mode si está activado
echo "   • Verificando modo seguro de HDFS..."
docker exec hadoop-namenode hdfs dfsadmin -safemode leave 2>/dev/null || echo "   - Safe mode ya desactivado o no aplicable"

# Limpiar directorio /data completamente
echo "   • Limpiando directorio /data existente..."
docker exec hadoop-namenode hdfs dfs -rm -r -f /data/* 2>/dev/null || echo "   - No había archivos en /data"
docker exec hadoop-namenode hdfs dfs -rm -r -f /data 2>/dev/null || echo "   - No existía directorio /data"

# Limpiar archivos residuales del producer de forma más agresiva
echo "   • Limpiando archivos residuales del producer..."
docker exec hadoop-namenode hdfs dfs -rm -f /data/input/*.csv 2>/dev/null || echo "   - No había archivos CSV residuales"
docker exec hadoop-namenode hdfs dfs -rm -f /data/processed/*.csv 2>/dev/null || echo "   - No había archivos procesados residuales"

# Esperar a que se completen las eliminaciones
sleep 5

# Crear estructura de directorios limpia
echo "   • Creando estructura de directorios limpia..."
docker exec hadoop-namenode hdfs dfs -mkdir -p /tmp
docker exec hadoop-namenode hdfs dfs -mkdir -p /user/hive/warehouse
docker exec hadoop-namenode hdfs dfs -mkdir -p /user/hive/tmp
docker exec hadoop-namenode hdfs dfs -mkdir -p /data/input
docker exec hadoop-namenode hdfs dfs -mkdir -p /data/processed

# Aplicar permisos
echo "   • Aplicando permisos..."
docker exec hadoop-namenode hdfs dfs -chmod -R 777 /tmp
docker exec hadoop-namenode hdfs dfs -chmod -R 777 /user/hive/warehouse
docker exec hadoop-namenode hdfs dfs -chmod -R 777 /user/hive/tmp
docker exec hadoop-namenode hdfs dfs -chmod -R 777 /data

# Verificar estructura creada - FORZAR lista vacía
echo "   • Verificando estructura HDFS limpia..."
docker exec hadoop-namenode hdfs dfs -ls -R /data/ 2>/dev/null && echo "   - Directorio /data/ contiene archivos (debería estar vacío)" || echo "   - Directorio /data/ recién creado y vacío"

# Paso 4: Iniciar Spark
echo "4. Iniciando Spark..."
docker-compose up -d spark-master spark-worker
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
    docker logs hive-metastore
    exit 1
fi

# Inicializar el esquema de metastore (solo si es necesario)
echo "Verificando esquema de Hive Metastore..."
if docker exec hive-metastore /opt/hive/bin/schematool -validate -dbType postgres 2>/dev/null; then
    echo "✓ Esquema de metastore ya está inicializado"
else
    echo "Inicializando esquema de metastore..."
    docker exec hive-metastore /opt/hive/bin/schematool -initSchema -dbType postgres
fi

echo "Esperando 5 segundos adicionales..."
sleep 5

# Verificar metastore
echo "Verificando Hive Metastore..."
if docker ps | grep -q hive-metastore; then
    echo "✓ Hive Metastore está corriendo"
    
    # Verificar puerto
    if docker exec hive-metastore netstat -tuln | grep -q 9083; then
        echo "✓ Hive Metastore escuchando en puerto 9083"
    else
        echo "✗ Hive Metastore no está escuchando en el puerto 9083"
        echo "Logs del metastore:"
        docker logs hive-metastore | tail -20
    fi
else
    echo "✗ Hive Metastore no está corriendo"
    exit 1
fi

# Paso 6: INICIALIZACIÓN CORREGIDA DE HIVE SERVER
echo "6. Inicializando Hive Server..."

# Iniciar el contenedor de hive server (ahora con las variables corregidas)
docker-compose up -d hive-server
echo "Esperando 5 segundos para Hive Server..."
sleep 5

# Verificar que el contenedor está corriendo
if ! docker ps | grep -q hive-server; then
    echo "✗ Error: Contenedor hive-server no está corriendo"
    docker logs hive-server
    exit 1
fi

# Verificar hiveserver2
echo "Verificando Hive Server..."
if docker ps | grep -q hive-server; then
    echo "✓ Hive Server está corriendo"
    
    # Verificar proceso hiveserver2
    if docker exec hive-server ps aux | grep -q [h]iveserver2; then
        echo "✓ Proceso hiveserver2 está ejecutándose"
    else
        echo "✗ Proceso hiveserver2 no está ejecutándose"
        echo "Intentando iniciar hiveserver2 manualmente..."
        docker exec -d hive-server /opt/hive/bin/hiveserver2
        sleep 10
    fi
    
    # Verificar puerto
    if docker exec hive-server netstat -tuln | grep -q 10000; then
        echo "✓ Hive Server escuchando en puerto 10000"
    else
        echo "✗ Hive Server no está escuchando en el puerto 10000"
        echo "Logs del hive server:"
        docker logs hive-server | tail -20
    fi
else
    echo "✗ Hive Server no está corriendo"
    exit 1
fi


# Paso adicional: Crear tabla inicial en Hive
echo "Creando tabla inicial en Hive..."
docker exec hive-server /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -n root -e "
CREATE DATABASE IF NOT EXISTS default;
USE default;

-- Tabla para datos brutos
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
) STORED AS ORC;

-- Vista para datos agregados
CREATE OR REPLACE VIEW retail_sales_aggregated AS
SELECT 
    date,
    category,
    region,
    SUM(units_sold) as total_units_sold,
    AVG(price) as avg_price,
    SUM(units_sold * price) as total_revenue,
    AVG(inventory_level) as avg_inventory
FROM retail_sales_raw 
GROUP BY date, category, region;

SHOW TABLES;
"



# Paso 7: Iniciar Data Producer y Streamlit App
echo "7. Iniciando Data Producer y Streamlit App..."
docker-compose up -d data-producer streamlit-app
sleep 5

# Paso 8: INICIAR SPARK-CONSUMER
echo "8. Iniciando Spark Consumer"
docker-compose up -d spark-consumer
echo "Esperando 10 segundos para inicialización..."
sleep 10

# Verificar nuevos contenedores
echo "Verificando nuevos contenedores..."
if docker ps | grep -q spark-consumer; then
    echo "✅ Spark Consumer está corriendo"
    echo "   • Mostrando logs iniciales:"
    docker logs spark-consumer --tail 5
else
    echo "❌ Spark Consumer no está corriendo"
fi

# Verificar nuevos contenedores
echo "Verificando nuevos contenedores..."
if docker ps | grep -q data-producer; then
    echo "✓ Data Producer está corriendo"
else
    echo "✗ Data Producer no está corriendo"
fi
if docker ps | grep -q streamlit-app; then
    echo "✓ Streamlit App está corriendo"
else
    echo "✗ Streamlit App no está corriendo"
fi

# Pruebas finales
echo "9. Realizando pruebas finales..."
echo ""
echo "=== ESTADO DE TODOS LOS SERVICIOS ==="
docker ps --format "table {{.Names}}\t{{.Status}}"

echo ""
echo "=== PRUEBA DE HIVE ==="

# Probar Hive con múltiples intentos
echo "Probando Hive Server..."
MAX_ATTEMPTS=12
SUCCESS=0

for i in $(seq 1 $MAX_ATTEMPTS); do
    echo "Intento $i de $MAX_ATTEMPTS..."
    
    # Probar con timeout y redirección de errores
    if timeout 10s docker exec hive-server /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -n root -e 'SHOW DATABASES;' 2>/dev/null; then
        echo ""
        echo "✓ ✓ ✓ ¡ÉXITO! HIVE FUNCIONA CORRECTAMENTE ✓ ✓ ✓"
        echo "✓ Metastore y HiveServer2 operativos"
        echo "✓ Se pueden ejecutar consultas HiveQL"
        SUCCESS=1
        break
    else
        echo "Hive aún no responde, esperando 5 segundos..."
        sleep 5
    fi
done

if [ $SUCCESS -eq 0 ]; then
    echo ""
    echo "✗ Hive no responde después de $MAX_ATTEMPTS intentos"
    echo ""
    echo "=== DIAGNÓSTICO AVANZADO ==="
    echo "1. Verificando conexión a PostgreSQL desde hive-metastore:"
    docker exec hive-metastore nc -zv postgres 5432
    echo ""
    echo "2. Verificando conexión a hive-metastore desde hive-server:"
    docker exec hive-server nc -zv hive-metastore 9083
    echo ""
    echo "3. Verificando conexión a HDFS desde hive-server:"
    docker exec hive-server nc -zv hadoop-namenode 8020
    echo ""
    echo "4. Últimos logs de Hive Metastore:"
    docker logs hive-metastore | tail -15
    echo ""
    echo "5. Últimos logs de Hive Server:"
    docker logs hive-server | tail -15
fi


#!/bin/bash
# postgres-hive-cleaner-quiet.sh - Limpia retail_sales en PostgreSQL y Hive sin preguntar

POSTGRES_TABLE="retail_sales"
HIVE_TABLE="retail_sales_raw"

echo "🧹 INICIANDO LIMPIEZA COMBINADA POSTGRESQL + HIVE"

# Función para verificar si un contenedor está corriendo
container_is_running() {
    docker ps --format '{{.Names}}' | grep -q "$1"
}

# Función para verificar si una tabla existe en Hive
hive_table_exists() {
    docker exec hive-server /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -n root -e "SHOW TABLES LIKE '$1';" 2>/dev/null | grep -q "$1"
}

# Función para obtener conteo de Hive
get_hive_count() {
    docker exec hive-server /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -n root -e "SELECT COUNT(*) FROM $1;" 2>/dev/null | grep -v "SLF4J" | grep -v "log4j" | grep -v "WARN" | grep -v "INFO" | grep -v "+" | grep -v "|" | grep -E '^[0-9]+$' | head -1 | tr -d ' \n'
}

# Iniciar PostgreSQL si no está corriendo
if ! container_is_running "postgres"; then
    echo "🚀 Iniciando PostgreSQL..."
    docker-compose up -d postgres > /dev/null 2>&1
    sleep 10
fi

# Iniciar Hive si no está corriendo
if ! container_is_running "hive-server"; then
    echo "🚀 Iniciando Hive..."
    docker-compose up -d hive-metastore hive-server > /dev/null 2>&1
    echo "⏳ Esperando inicialización de Hive..."
    sleep 20
fi

# Verificar que Hive esté respondiendo
echo "🔍 Verificando estado de Hive..."
MAX_ATTEMPTS=5
HIVE_READY=false

for i in $(seq 1 $MAX_ATTEMPTS); do
    if docker exec hive-server /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -n root -e "SHOW DATABASES;" >/dev/null 2>&1; then
        HIVE_READY=true
        break
    fi
    echo "   Intento $i/$MAX_ATTEMPTS - Hive no responde, esperando 5s..."
    sleep 5
done

if [ "$HIVE_READY" = false ]; then
    echo "⚠️  Hive no está respondiendo, continuando solo con PostgreSQL..."
fi

# Obtener conteos ANTES de la limpieza
echo "📊 Conteos antes de la limpieza:"

POSTGRES_COUNT_BEFORE=$(docker exec postgres psql -U hive -d hive -t -c "SELECT COUNT(*) FROM $POSTGRES_TABLE;" 2>/dev/null | tr -d ' \n' || echo "0")
echo "   PostgreSQL $POSTGRES_TABLE: $POSTGRES_COUNT_BEFORE registros"

if [ "$HIVE_READY" = true ]; then
    HIVE_COUNT_BEFORE=$(get_hive_count "$HIVE_TABLE" || echo "0")
    echo "   Hive $HIVE_TABLE: $HIVE_COUNT_BEFORE registros"
fi

# Limpiar PostgreSQL
echo ""
echo "🗑️  Limpiando PostgreSQL..."
docker exec postgres psql -U hive -d hive -c "TRUNCATE TABLE $POSTGRES_TABLE;" > /dev/null 2>&1

if [ $? -eq 0 ]; then
    POSTGRES_COUNT_AFTER=$(docker exec postgres psql -U hive -d hive -t -c "SELECT COUNT(*) FROM $POSTGRES_TABLE;" | tr -d ' \n')
    echo "✅ PostgreSQL $POSTGRES_TABLE limpiada ($POSTGRES_COUNT_AFTER registros restantes)"
else
    echo "❌ Error limpiando PostgreSQL"
    exit 1
fi

# Limpiar Hive (si está disponible)
if [ "$HIVE_READY" = true ]; then
    echo ""
    echo "🗑️  Limpiando Hive..."
    
    # Verificar si la tabla existe
    if hive_table_exists "$HIVE_TABLE"; then
        # Opción 1: TRUNCATE (si funciona)
        docker exec hive-server /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -n root -e "TRUNCATE TABLE $HIVE_TABLE;" > /dev/null 2>&1
        
        if [ $? -eq 0 ]; then
            HIVE_COUNT_AFTER=$(get_hive_count "$HIVE_TABLE" || echo "0")
            echo "✅ Hive $HIVE_TABLE limpiada ($HIVE_COUNT_AFTER registros restantes)"
        else
            # Opción 2: DROP y CREATE (más agresivo)
            echo "   TRUNCATE falló, intentando DROP + CREATE..."
            docker exec hive-server /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -n root -e "DROP TABLE $HIVE_TABLE;" > /dev/null 2>&1
            
            # Recrear la tabla
            docker exec hive-server /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -n root -e "
                CREATE TABLE $HIVE_TABLE (
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
                ) STORED AS ORC;" > /dev/null 2>&1
            
            HIVE_COUNT_AFTER=$(get_hive_count "$HIVE_TABLE" || echo "0")
            echo "✅ Hive $HIVE_TABLE recreada ($HIVE_COUNT_AFTER registros restantes)"
        fi
    else
        echo "ℹ️  Tabla $HIVE_TABLE no existe en Hive, creándola..."
        docker exec hive-server /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -n root -e "
            CREATE TABLE $HIVE_TABLE (
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
            ) STORED AS ORC;" > /dev/null 2>&1
        echo "✅ Tabla $HIVE_TABLE creada en Hive"
    fi
fi

# Verificación final
echo ""
echo "📋 RESUMEN FINAL:"
echo "   PostgreSQL $POSTGRES_TABLE: $POSTGRES_COUNT_AFTER registros"

if [ "$HIVE_READY" = true ]; then
    HIVE_FINAL_COUNT=$(get_hive_count "$HIVE_TABLE" || echo "0")
    echo "   Hive $HIVE_TABLE: $HIVE_FINAL_COUNT registros"
    
    # Verificar consistencia
    if [ "$POSTGRES_COUNT_AFTER" = "$HIVE_FINAL_COUNT" ]; then
        echo "✅ ✅ ✅ ¡CONSISTENTE! Ambas bases de datos tienen los mismos registros"
    else
        echo "⚠️  ⚠️  ⚠️  ¡INCONSISTENTE! Diferencia de registros entre bases de datos"
    fi
else
    echo "   Hive: No disponible para verificación"
fi

echo ""
echo "🎯 LIMPIEZA COMBINADA COMPLETADA"



echo ""
echo "=== URLs DE ACCESO ==="
echo "HDFS NameNode: http://localhost:9870"
echo "YARN ResourceManager: http://localhost:8088" 
echo "Spark Master: http://localhost:8080"
echo "Hive Server: localhost:10000"
echo "Streamlit App: http://localhost:8501"

echo ""
echo "=== COMANDOS ÚTILES ==="
echo "Conectar a Hive: docker exec -it hive-server /opt/hive/bin/beeline -u jdbc:hive2://localhost:10000 -n root"
echo "Ver logs Data Producer: docker logs data-producer -f"
echo "Ver logs Streamlit App: docker logs streamlit-app -f"
echo "Ver logs Hive Server: docker logs hive-server -f"
echo "Ver logs Hive Metastore: docker logs hive-metastore -f"
echo "Probar HDFS: docker exec hadoop-namenode hdfs dfs -ls /"
echo "Reiniciar solo Hive: docker-compose restart hive-metastore hive-server"
echo "Limpiar HDFS: docker exec hadoop-namenode hdfs dfs -rm -r -f /data && docker exec hadoop-namenode hdfs dfs -mkdir -p /data/input /data/processed"

echo ""
echo "=== ESTRUCTURA DE DIRECTORIOS ==="
echo "Asegúrate de tener esta estructura de archivos:"
echo "./"
echo "├── docker-compose.yml"
echo "├── setup.sh"
echo "├── config/           # Archivos de configuración"
echo "├── dataset/          # CSV de datos de entrada"
echo "├── producer/         # Script data-producer.py"
echo "├── consumer/         # Script PySpark consumer.py" 
echo "└── streamlit/        # Script app.py de Streamlit"

echo ""
echo "=== VERIFICACIÓN FINAL HDFS ==="
echo "Espacio utilizado en /data:"
docker exec hadoop-namenode hdfs dfs -du -h /data/ 2>/dev/null || echo "Directorio /data/ vacío"
echo "Archivos en /data/input:"
docker exec hadoop-namenode hdfs dfs -ls /data/input/ 2>/dev/null || echo "No hay archivos en /data/input"

echo ""
echo "=== CLUSTER INICIADO CON HDFS LIMPIO ==="