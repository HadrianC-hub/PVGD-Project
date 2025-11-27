import pandas as pd
import subprocess
import time
import os
import re
import sys
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import pickle
from collections import defaultdict, deque

# CONFIGURACIÓN
BATCH_SIZE = 5000        # Tamaño del batch
SLEEP_TIME = 0.5         # Tiempo entre lotes

# CARGA DATASET BASE
BASE_DATASET_PATH = "/dataset/data.csv"
BASE_DF = pd.read_csv(BASE_DATASET_PATH)

BASE_DF.columns = [c.strip() for c in BASE_DF.columns]

# Ruta para guardar el estado del forecast (persistente entre reinicios)
FORECAST_STATE_PATH = "/producer/forecast_state.pkl"
# Cuántas observaciones por (store,product) mantenemos
HISTORY_WINDOW = 28  # días/batches guardados
# Cada cuántos batches persistimos el estado en disco
PERSIST_EVERY = 10

def default_forecast_state():
    return deque(maxlen=HISTORY_WINDOW)

# Estructura: state[(store_id, product_id)] = deque([units_sold_history...], maxlen=HISTORY_WINDOW)
if os.path.exists(FORECAST_STATE_PATH):
    try:
        with open(FORECAST_STATE_PATH, "rb") as f:
            FORECAST_STATE = pickle.load(f)
            # convertir listas en deques si vienen así
            for k, v in list(FORECAST_STATE.items()):
                if not isinstance(v, deque):
                    FORECAST_STATE[k] = deque(v, maxlen=HISTORY_WINDOW)
    except Exception as e:
        print("WARN: no se pudo cargar forecast_state, inicializando vacío:", e)
        FORECAST_STATE = defaultdict(default_forecast_state)
else:
    FORECAST_STATE = defaultdict(default_forecast_state)

def persist_forecast_state():
    try:
        tmp_path = FORECAST_STATE_PATH + ".tmp"
        with open(tmp_path, "wb") as f:
            pickle.dump(FORECAST_STATE, f)
        os.replace(tmp_path, FORECAST_STATE_PATH)
    except Exception as e:
        print("WARN: fallo al persistir forecast state:", e)

def check_hdfs_ready():
    """Verificar si HDFS está listo"""
    for i in range(30):
        try:
            subprocess.check_output(["hdfs", "dfs", "-test", "-e", "/"], stderr=subprocess.STDOUT)
            print("HDFS está listo y respondiendo")
            return True
        except:
            pass
        print(f"Esperando HDFS... ({i+1}/{30})")
        time.sleep(5)
    return False

def setup_hdfs_directories():
    """Crear directorios necesarios en HDFS"""
    commands = [
        ["hdfs", "dfs", "-mkdir", "-p", "/data/input"],
        ["hdfs", "dfs", "-mkdir", "-p", "/data/processed"],
        ["hdfs", "dfs", "-chmod", "-R", "777", "/data"]
    ]
    for cmd in commands:
        try:
            subprocess.run(cmd, capture_output=True)
        except:
            pass
    print("Directorios HDFS verificados")

def generate_fast_batch(batch_size, batch_number):

    current_date = datetime.now().strftime("%Y-%m-%d")

    # GENERACIÓN DIMENSIONAL
    batch = (
        BASE_DF
            .sample(n=batch_size, replace=True)
            .copy()
            .reset_index(drop=True)
    )

    # Introduciendo fecha actual
    batch["Date"] = current_date

    # Modificando valores con distribución normal
    batch["Units Sold"] *= np.random.normal(1.0, 0.12, batch_size)
    batch["Inventory Level"] *= np.random.normal(1.0, 0.15, batch_size)
    batch["Demand Forecast"] = batch["Units Sold"] * np.random.normal(1.03, 0.10, batch_size)
    batch["Price"] *= np.random.normal(1.0, 0.05, batch_size)
    batch["Competitor Pricing"] = batch["Price"] * np.random.normal(1.0, 0.08, batch_size)

    # Modificando valores con coeficiente de estación
    season_factor = batch["Seasonality"].map({
        "Summer": 1.35,
        "Spring": 1.10,
        "Autumn": 1.05,
        "Winter": 0.85
    }).fillna(1.0)

    batch["Units Sold"] *= season_factor
    batch["Demand Forecast"] *= season_factor

    promo = np.where(batch["Holiday/Promotion"] == 1, 1.5, 1.0)
    batch["Units Sold"] *= promo
    batch["Units Sold"] *= (1 + batch["Discount"] * 0.02)

    reorder_mask = batch["Inventory Level"] < batch["Units Sold"] * 1.2

    batch.loc[reorder_mask, "Units Ordered"] = np.random.randint(30,200,reorder_mask.sum())

    for col in ["Units Sold", "Inventory Level", "Units Ordered"]:
        batch[col] = batch[col].round(0).astype(int)

    batch["Demand Forecast"] = batch["Demand Forecast"].round(2)
    batch["Price"] = batch["Price"].round(2)
    batch["Competitor Pricing"] = batch["Competitor Pricing"].round(2)

    batch.columns = (
        batch.columns.str.lower()
                      .str.replace(" ", "_")
                      .str.replace("/", "_")
    )

    # FORECAST INCREMENTAL

    # Parámetros EMA (35% de importancia a la observación actual y 65% al histórico suavizado)
    alpha = 0.35

    # Pesos de cada componente
    w_ema = 0.60      # Tendencia (EMA)
    w_mean = 0.30     # Media móvil
    w_exog = 0.10     # Factores externos

    # Vector donde se almacenan los forecasts
    forecasts = np.zeros(len(batch), dtype=float)

    # Procesa cada registro del batch
    for i, row in batch.iterrows():

        # Identifica la serie tienda-producto
        key = (row["store_id"], row["product_id"])

        # Última venta observada
        last_sale = int(row["units_sold"])

        # Obtiene el histórico de ventas
        hist = FORECAST_STATE.get(key, deque(maxlen=HISTORY_WINDOW))

        # EMA (suavizado exponencial)
        if len(hist) >= 1:
            ema_val = hist[0]
            for v in list(hist)[1:]:
                ema_val = alpha * v + (1 - alpha) * ema_val

            # Añade el último valor
            ema = alpha * last_sale + (1 - alpha) * ema_val
        else:
            ema = last_sale

        # Media móvil (ventana 7)
        if len(hist) >= 7:
            mean_k = np.mean(list(hist)[-7:])
        elif len(hist) > 0:
            mean_k = np.mean(hist)
        else:
            mean_k = last_sale

        # Ajuste por precio vs competencia
        comp = max(1.0, row["competitor_pricing"])
        price_rel = row["price"] / comp

        # Función exponencial de penalización/beneficio
        price_adj = np.exp(-0.08 * (price_rel - 1.0))

        # Ajuste por promoción
        promo_adj = 1.25 if row["holiday_promotion"] == 1 else 1.0

        # Forecast combinado
        raw_forecast = (
            w_ema * ema +
            w_mean * mean_k +
            w_exog * (last_sale * price_adj * promo_adj)
        )

        # Ruido aleatorio (±3%)
        raw_forecast *= np.random.normal(1.0, 0.03)

        # Evita valores negativos y redondea
        forecasts[i] = max(0.0, round(raw_forecast, 2))

        # Actualiza histórico
        if key not in FORECAST_STATE:
            FORECAST_STATE[key] = deque(maxlen=HISTORY_WINDOW)

        FORECAST_STATE[key].append(last_sale)

    # Añade la columna al dataframe
    batch["demand_forecast"] = forecasts

    # Guardado periódico en disco
    if batch_number % PERSIST_EVERY == 0:
        try:
            persist_forecast_state()
        except Exception as e:
            print("WARN: persist failed", e)

    return batch

def _normalize_column_name(col: str) -> str:
    """Normaliza un nombre de columna a snake_case seguro para Parquet/Hive."""
    if not isinstance(col, str):
        col = str(col)
    # Lowercase + strip
    c = col.strip().lower()
    # Reemplazar barras y espacios por guión bajo
    c = c.replace("/", "_").replace(" ", "_").replace("-", "_")
    # Eliminar puntos
    c = c.replace(".", "")
    # Eliminar caracteres no válidos (incluye los señalados por Spark)
    c = re.sub(r'[;{}\(\)\n\t=]', '', c)
    # Reemplazar cualquier secuencia de caracteres no alfanuméricos por underscore
    c = re.sub(r'[^0-9a-z_]', '_', c)
    # Colapsar underscores repetidos
    c = re.sub(r'__+', '_', c)
    # Quitar underscores al inicio/fin
    c = c.strip('_')
    # Si queda vacío, poner generic_col
    if c == "":
        c = "col"
    return c

def upload_batch_to_hdfs(batch_df, batch_number):
    """Sube el batch a HDFS con normalización segura de columnas para Hive/Parquet."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    date_partition = datetime.now().strftime("%Y-%m-%d")
    filename = f"retail_batch_{batch_number}_{timestamp}.parquet"
    
    local_path = f"/tmp/{filename}"
    hdfs_dest = f"/data/input/date={date_partition}/{filename}"
    
    try:
        # Normalizar columnas para evitar problemas con Hive/Parquet
        new_cols = [_normalize_column_name(c) for c in batch_df.columns]
        batch_df.columns = new_cols
        
        # Convertir a tabla PyArrow y guardar localmente
        table = pa.Table.from_pandas(batch_df, preserve_index=False)
        pq.write_table(table, local_path, compression='snappy')
        
        # Crear carpeta de partición si es el primer batch del día
        if batch_number % 100 == 0:
             subprocess.run(["hdfs", "dfs", "-mkdir", "-p", f"/data/input/date={date_partition}"], capture_output=True)

        # Subir a HDFS
        put_result = subprocess.run(
            ["hdfs", "dfs", "-put", "-f", local_path, hdfs_dest],
            capture_output=True, text=True
        )
        
        # Limpiar local
        if os.path.exists(local_path):
            os.remove(local_path)
            
        if put_result.returncode == 0:
            return True
        else:
            print(f"Error HDFS: {put_result.stderr}")
            return False
            
    except Exception as e:
        print(f"Error crítico subiendo lote: {e}")
        return False

def main():
    print("INICIANDO HIGH-PERFORMANCE DATA PRODUCER")
    print(f"Configuración: {BATCH_SIZE} registros cada {SLEEP_TIME}s")
    print(f"Volumen estimado: {int(BATCH_SIZE * (1/SLEEP_TIME) * 60):,} registros/minuto")
    
    # Comprobando que HDFS esté disponible
    if not check_hdfs_ready():
        sys.exit(1)
        
    # Creando directorios para datos en HDFS
    setup_hdfs_directories()
    
    batch_counter = 0
    total_records = 0
    
    try:
        while True:
            start_time = time.time()
            
            # 1. Generar dataframe
            df = generate_fast_batch(BATCH_SIZE, batch_counter)
            gen_time = time.time() - start_time
            
            # 2. Subir dataframe a HDFS
            upload_start = time.time()
            success = upload_batch_to_hdfs(df, batch_counter)
            upload_time = time.time() - upload_start
            
            if success:
                total_records += len(df)
                print(f"Lote {batch_counter} OK | Generación: {gen_time:.3f}s | Subida: {upload_time:.3f}s | Total: {total_records:,} regs")
            
            batch_counter += 1
            
            # Descanso dinámico (si el proceso fue muy rápido, dormimos lo configurado)
            # Si el proceso fue lento, no dormimos para intentar recuperar tiempo
            total_cycle_time = time.time() - start_time
            if total_cycle_time < SLEEP_TIME:
                time.sleep(SLEEP_TIME - total_cycle_time)
                
    except KeyboardInterrupt:
        print("\nProducción detenida.")

if __name__ == "__main__":
    main()