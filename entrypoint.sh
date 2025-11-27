#!/bin/bash
set -e

# Configurar CLASSPATH para que PyArrow encuentre las librerías de Hadoop
export CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath --glob)

echo "✅ Entorno Hadoop configurado."
echo "   • JAVA_HOME: $JAVA_HOME"
echo "   • HADOOP_HOME: $HADOOP_HOME"
echo "   • CLASSPATH generado correctamente"

# Ejecutar el comando que le pase Docker (streamlit run ...)
exec "$@"