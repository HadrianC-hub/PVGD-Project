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

def load_and_analyze_dataset():
    """Cargar y analizar el dataset real con limpieza inicial"""
    dataset_path = '/dataset/data.csv'
    if os.path.exists(dataset_path):
        df = pd.read_csv(dataset_path)
        
        # Limpieza inicial del dataset base
        print("🔧 Realizando limpieza inicial del dataset...")
        
        # Renombrar columnas problemáticas
        df.columns = [col.replace(' ', '_').replace('/', '_').replace('-', '_') for col in df.columns]
        
        # Limpiar valores nulos
        numeric_cols = ['Inventory_Level', 'Units_Sold', 'Units_Ordered', 'Demand_Forecast', 'Price', 'Discount', 'Competitor_Pricing']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0)
        
        # Limpiar columnas de texto
        text_cols = ['Store_ID', 'Product_ID', 'Category', 'Region', 'Weather_Condition', 'Seasonality']
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].fillna('Unknown').str.strip()
        
        print("📊 Dataset real cargado y limpiado: {} registros".format(len(df)))
        print("🔍 Estructura detectada:")
        print("   - Columnas: {}".format(list(df.columns)))
        print("   - Tipos de datos:")
        for col in df.columns:
            print("     * {}: {}".format(col, df[col].dtype))
        print("   - Muestra de datos limpios:")
        print(df.head(2))
        return df
    else:
        print("❌ Error: No se encuentra el dataset en /dataset/data.csv")
        sys.exit(1)

def generate_batch_data(base_df, batch_size=100, batch_number=0):
    """Generar un lote de datos nuevo basado en el dataset real"""
    # Tomar una muestra aleatoria del dataset base
    sample_size = min(batch_size, len(base_df))
    sample = base_df.sample(n=sample_size, replace=False).copy()
    
    # Modificar la fecha para que sea actual
    current_date = datetime.now().strftime("%Y-%m-%d")
    sample['Date'] = current_date
    
    # Modificar valores numéricos para simular nuevos datos
    numeric_columns = ['Inventory_Level', 'Units_Sold', 'Units_Ordered', 'Demand_Forecast', 'Price', 'Discount', 'Competitor_Pricing']
    
    for col in numeric_columns:
        if col in sample.columns:
            # Añadir variación aleatoria (±15%)
            variation = random.uniform(0.85, 1.15)
            if col in ['Inventory_Level', 'Units_Sold', 'Units_Ordered']:
                # Para valores enteros, redondear y asegurar positivos
                sample[col] = (sample[col] * variation).round().astype(int).clip(lower=0)
            else:
                # Para valores decimales, mantener decimales y asegurar positivos
                sample[col] = (sample[col] * variation).round(2).clip(lower=0)
    
    # Modificar categorías/texto ocasionalmente para variedad
    text_columns = ['Store_ID', 'Product_ID', 'Category', 'Region', 'Weather_Condition', 'Seasonality']
    
    for col in text_columns:
        if col in sample.columns and random.random() > 0.7:  # 30% de probabilidad
            if col == 'Store_ID':
                sample[col] = 'S' + sample[col].str[1:].apply(lambda x: "{:03d}".format(int(x)))
            elif col == 'Product_ID':
                sample[col] = 'P' + sample[col].str[1:].apply(lambda x: "{:04d}".format(int(x)))
            elif col == 'Category':
                categories = ['Groceries', 'Toys', 'Electronics', 'Furniture', 'Clothing', 'Sports', 'Books', 'Home_Appliances']
                sample[col] = random.choices(categories, k=len(sample))
            elif col == 'Region':
                regions = ['North', 'South', 'East', 'West', 'Central', 'Northeast', 'Southwest']
                sample[col] = random.choices(regions, k=len(sample))
            elif col == 'Weather_Condition':
                weathers = ['Sunny', 'Cloudy', 'Rainy', 'Snowy', 'Windy', 'Foggy', 'Stormy']
                sample[col] = random.choices(weathers, k=len(sample))
            elif col == 'Seasonality':
                seasons = ['Spring', 'Summer', 'Autumn', 'Winter']
                sample[col] = random.choices(seasons, k=len(sample))
    
    # Modificar Holiday_Promotion aleatoriamente
    if 'Holiday_Promotion' in sample.columns:
        sample['Holiday_Promotion'] = [random.choice([0, 1]) for _ in range(len(sample))]
    
    return sample

def upload_batch_to_hdfs(batch_df, batch_number):
    """Subir un lote de datos a HDFS como archivo separado"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = "retail_batch_{}_{}.csv".format(batch_number, timestamp)
    local_batch_path = "/dataset/{}".format(filename)
    hdfs_batch_path = "/data/input/{}".format(filename)
    
    try:
        # Asegurar que las columnas tengan nombres limpios
        batch_df.columns = [col.replace(' ', '_').replace('/', '_').replace('-', '_') for col in batch_df.columns]
        
        # Guardar lote localmente
        batch_df.to_csv(local_batch_path, index=False)
        
        # Subir a HDFS
        put_cmd = ["hdfs", "dfs", "-put", "-f", local_batch_path, hdfs_batch_path]
        result = subprocess.run(put_cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Lote {} subido a HDFS: {} ({} registros)".format(batch_number, hdfs_batch_path, len(batch_df)))
            # Eliminar archivo local temporal
            os.remove(local_batch_path)
            return True
        else:
            print("❌ Error subiendo lote {}: {}".format(batch_number, result.stderr))
            return False
            
    except Exception as e:
        print("❌ Error procesando lote {}: {}".format(batch_number, e))
        return False

def consolidate_data(batch_number):
    """Consolidar datos antiguos periódicamente"""
    try:
        if batch_number % 20 == 0 and batch_number > 0:
            print("🔄 Realizando consolidación periódica...")
            
            list_cmd = ["hdfs", "dfs", "-ls", "/data/input/retail_batch_*.csv"]
            result = subprocess.run(list_cmd, capture_output=True, text=True)
            
            if result.returncode == 0 and result.stdout.strip():
                files = [line.split()[-1] for line in result.stdout.strip().split('\n') if line]
                if len(files) > 10:
                    files_to_move = files[:-10]
                    for file_path in files_to_move:
                        move_cmd = ["hdfs", "dfs", "-mv", file_path, "/data/processed/"]
                        subprocess.run(move_cmd, capture_output=True)
                    print("📦 {} archivos antiguos movidos a /data/processed/".format(len(files_to_move)))
            
    except Exception as e:
        print("⚠️  Error en consolidación: {}".format(e))

def main():
    print("🚀 Iniciando Retail Data Producer Continuo...")
    print("📊 Dataset: Ventas minoristas (Retail)")
    print("⏰ Modo: Producción por lotes cada 30 segundos")
    print("🔧 Característica: Datos limpios y normalizados")
    
    if not check_hdfs_ready():
        print("❌ HDFS no disponible después de 150 segundos")
        sys.exit(1)
    
    setup_hdfs_directories()
    
    base_df = load_and_analyze_dataset()
    
    batch_number = 0
    
    print("\n🎯 Iniciando producción de datos de retail...")
    print("   • Lote cada: 30 segundos")
    print("   • Tamaño de lote: 50-150 registros")
    print("   • Consolidación cada: 20 lotes\n")
    
    try:
        while True:
            batch_size = random.randint(50, 150)
            
            print("\n📦 Generando lote {}...".format(batch_number))
            print("   • Tamaño: {} registros".format(batch_size))
            print("   • Timestamp: {}".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            
            batch_df = generate_batch_data(base_df, batch_size, batch_number)
            
            if 'Category' in batch_df.columns:
                category_counts = batch_df['Category'].value_counts()
                print("   • Distribución por categoría: {}".format(dict(category_counts)))
            
            if 'Units_Sold' in batch_df.columns:
                total_sold = batch_df['Units_Sold'].sum()
                print("   • Total unidades vendidas: {}".format(total_sold))
            
            success = upload_batch_to_hdfs(batch_df, batch_number)
            
            if success:
                consolidate_data(batch_number)
                
                total_records_approx = (batch_number + 1) * batch_size
                print("   • Total acumulado aproximado: ~{} registros".format(total_records_approx))
                print("   • Próximo lote en: 30 segundos")
            
            batch_number += 1
            time.sleep(30)
            
    except KeyboardInterrupt:
        print("\n\n🛑 Producer detenido por el usuario")
        print("📈 Resumen: {} lotes procesados".format(batch_number))
        sys.exit(0)

if __name__ == "__main__":
    main()