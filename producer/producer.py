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
