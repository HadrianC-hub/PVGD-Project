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