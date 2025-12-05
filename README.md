# Optimización de Inventarios y Logística en Retail

Sistema distribuido para procesamiento masivo de datos de ventas retail, orientado a predicción de demanda, análisis de costos logísticos y detección de alertas de inventario, utilizando HDFS, Spark, Hive y visualización en Streamlit sobre contenedores Docker.

---

## Requisitos para Ejecutar el Proyecto

Antes de levantar la plataforma es necesario contar con:

- **Sistema operativo:** Linux, macOS o Windows 10/11 (con WSL2 recomendado)
- **Docker Engine** >= 20.10
- **Docker Compose** >= 2.0
- **Bash** (para ejecutar `setup.sh`)
- **Mínimo recomendado de recursos:**
  - 8 GB de RAM (mínimo absoluto: 6 GB)
  - 4 CPUs
  - 12 GB de espacio libre en disco

---

## Puesta en Marcha

Para construir y lanzar todo el entorno distribuido:

```bash
./setup.sh
```
Para detener el proyecto, desde la carpeta raíz del proyecto ejecutar:
```bash
docker-compose down
```
El archivo *setup.sh* realiza una ejecución ordenada de todos los contenedores definidos en docker-compose.