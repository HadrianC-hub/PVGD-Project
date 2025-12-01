import streamlit as st
import pandas as pd
import plotly.express as px
from pyarrow import fs
import pyarrow.parquet as pq
import time
from datetime import datetime, timedelta

# Configuración de la página
st.set_page_config(
    page_title="Retail Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# === CONFIGURACIÓN ===
HDFS_NAMENODE_HOST = 'hadoop-namenode'
HDFS_PORT = 8020
HDFS_TABLE_PATH = "/user/hive/warehouse/retail_sales_raw"

# Columnas estrictamente necesarias para los gráficos
REQUIRED_COLUMNS = [
    'date', 'category', 'region', 'units_sold', 'inventory_level', 
    'demand_forecast', 'price', 'competitor_pricing', 'holiday_promotion'
]

@st.cache_data(ttl=60)
def get_dataset_metadata():
    """Obtiene solo metadatos ligeros para poblar los filtros sin leer datos."""
    try:
        hdfs = fs.HadoopFileSystem(HDFS_NAMENODE_HOST, HDFS_PORT)
        dataset = pq.ParquetDataset(HDFS_TABLE_PATH, filesystem=hdfs)
        categories = dataset.read(columns=['category']).to_pandas()['category'].unique()
        regions = dataset.read(columns=['region']).to_pandas()['region'].unique()
        return list(categories), list(regions)
    except Exception:
        return [], []

@st.cache_data(ttl=10)
def load_filtered_data(start_date, end_date, selected_cat, selected_reg):
    """Lee SOLO los datos necesarios usando pq.read_table."""

    try:
        # Conexión HDFS
        hdfs = fs.HadoopFileSystem(HDFS_NAMENODE_HOST, HDFS_PORT)
        
        # 1. Construir filtros (Lista de tuplas para PyArrow)
        # Esto permite que HDFS solo nos envíe las filas que cumplen la condición
        filters = []
        
        # Filtro de fecha (asumiendo formato YYYY-MM-DD en el parquet)
        filters.append(('date', '>=', start_date.strftime('%Y-%m-%d')))
        filters.append(('date', '<=', end_date.strftime('%Y-%m-%d')))
        
        if selected_cat != 'Todos':
            filters.append(('category', '==', selected_cat))
        
        if selected_reg != 'Todas':
            filters.append(('region', '==', selected_reg))
            
        # 2. Leer usando read_table
        table = pq.read_table(
            HDFS_TABLE_PATH,
            filesystem=hdfs,
            columns=REQUIRED_COLUMNS,
            filters=filters
        )
        
        # Convertir a Pandas
        df = table.to_pandas()
        
        if df.empty:
            return pd.DataFrame()
        
        # Asegurar tipos
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')

        # Cálculos vectorizados
        if 'price' in df.columns and 'units_sold' in df.columns:
            df['revenue'] = df['price'] * df['units_sold']

        if 'demand_forecast' in df.columns:
            # Error porcentual correcto basado en valores reales
            error_pct = abs(df['demand_forecast'] - df['units_sold']) / df['units_sold'].replace(0, 1)
            # Accuracy = 100 - MAPE
            df['forecast_accuracy'] = (100 - error_pct * 100).clip(0, 100)

        # Métricas
        if 'inventory_level' in df.columns:
            df['inventory_turnover'] = df['units_sold'] / df['inventory_level'].replace(0, 1)
            df['logistics_cost'] = df['inventory_level'] * 0.15 * 1.2

        return df

    except Exception as e:
        st.error(f"Error leyendo datos: {str(e)}")
        # Imprimir error en logs del contenedor para debug
        print(f"DEBUG ERROR: {e}")
        return pd.DataFrame()
    
@st.cache_data(ttl=5)
def load_data_from_hdfs():
    """Lee archivos Parquet directamente desde HDFS usando PyArrow."""
    try:
        # 1. Conectar al sistema de archivos distribuido
        hdfs = fs.HadoopFileSystem(HDFS_NAMENODE_HOST, HDFS_PORT)
        
        # 2. Intentamos crear el ParquetDataset directamente desde el directorio raíz.
        # Esto le permite a PyArrow buscar los metadatos y archivos automáticamente.
        dataset = pq.ParquetDataset(HDFS_TABLE_PATH,filesystem=hdfs,)
        
        # 3. Leer todo el dataset en un DataFrame de Pandas
        df = dataset.read().to_pandas()
        
        if df.empty:
            st.warning("Se pudo conectar, pero el DataFrame resultante está vacío.")
            return pd.DataFrame()
        
        # Procesamiento
        df.columns = [c.lower() for c in df.columns]

        if 'price' in df.columns and 'units_sold' in df.columns:
            df['revenue'] = df['price'] * df['units_sold']

        if 'demand_forecast' in df and 'units_sold' in df:
            # Error porcentual correcto basado en valores reales
            error_pct = abs(df['demand_forecast'] - df['units_sold']) / df['units_sold'].replace(0, 1)
            # Accuracy = 100 - MAPE
            df['forecast_accuracy'] = (100 - error_pct * 100).clip(0, 100)

        if 'date' in df.columns:
            # Aseguramos que 'date' se parsea a datetime
            df['date'] = pd.to_datetime(df['date'], errors='coerce')

        st.success(f"¡Datos leídos exitosamente! Total de registros: {len(df):,}")
        return df

    except Exception as e:
        # Este error ahora solo capturará fallos de I/O o de parsing de Parquet
        st.error(f"Error al intentar leer el Dataset Parquet: {type(e).__name__}: {str(e)}")
        return pd.DataFrame()

def calculate_logistics_costs(df):
    """
    Simula costos logísticos basados en:
    - Distancia por región (costo fijo)
    - Volumen de inventario (costo variable)
    - Tipo de producto (costo categoría)
    """
    # Costos base por región (simulados)
    region_costs = {'North': 1.2, 'South': 1.0, 'East': 1.3, 'West': 1.4, 'Central': 1.1, 'Northeast': 1.5, 'Southwest': 1.2}
    
    # Costos por categoría (simulados)
    category_costs = {'Electronics': 1.8, 'Groceries': 1.0, 'Clothing': 1.2,'Furniture': 2.0, 'Toys': 1.3, 'Sports': 1.4, 'Books': 1.1}
    
    # Calcular costos logísticos simulados
    df['base_logistics_cost'] = df['region'].map(region_costs).fillna(1.2)
    df['category_cost_multiplier'] = df['category'].map(category_costs).fillna(1.2)
    df['logistics_cost'] = (
        df['base_logistics_cost'] * df['category_cost_multiplier'] * df['inventory_level'] * 0.1  # Costo por unidad de inventario
    )
    
    return df

def calculate_efficiency_metrics(df):
    """Calcula métricas de eficiencia"""
    # Eficiencia de inventario
    df['inventory_turnover'] = df['units_sold'] / df['inventory_level'].replace(0, 1)
    
    # Eficiencia de precio vs competencia
    df['pricing_efficiency'] = ((df['price'] - df['competitor_pricing']) / df['competitor_pricing'].replace(0, 1) * 100)
    
    # Eficiencia de promociones
    df['promotion_efficiency'] = df['units_sold'] * df['holiday_promotion']
    
    return df

def main():
    st.title("🏭 Retail Analytics - Scalable Dashboard")
    
    # 1. SIDEBAR PRIMERO (Para definir qué cargar)
    st.sidebar.subheader("🎛️ Filtros de Carga")
    st.sidebar.info("Filtrar ANTES de cargar evita caídas de memoria.")
    
    # Obtener metadatos ligeros
    cat_opts, reg_opts = get_dataset_metadata()
    categories = ['Todos'] + sorted(cat_opts) if cat_opts else ['Todos']
    regions = ['Todas'] + sorted(reg_opts) if reg_opts else ['Todas']
    
    # Selectores
    selected_category = st.sidebar.selectbox("Categoría", categories)
    selected_region = st.sidebar.selectbox("Región", regions)
    
    # Fechas (Default: Últimos 7 días para no saturar de inicio)
    today = datetime.now().date()
    default_start = today - timedelta(days=7)
    
    date_range = st.sidebar.date_input(
        "Rango de Fechas",
        [default_start, today],
        max_value=today
    )
    
    if len(date_range) != 2:
        st.warning("Selecciona un rango de fechas completo.")
        st.stop()
        
    start_date, end_date = date_range

    # Botón de refresco manual
    if st.sidebar.button('🔄 Recargar Datos'):
        st.cache_data.clear()
        st.rerun()

    with st.sidebar:
        auto_refresh = st.checkbox(
            "🔁 Actualizar automáticamente",
            value=False,
            help="Recarga los datos cada 60 segundos sin perder filtros"
        )

    # 2. CARGA DE DATOS FILTRADOS
    with st.spinner(f"📡 Cargando datos ({start_date} a {end_date})..."):
        df = load_filtered_data(start_date, end_date, selected_category, selected_region)
    
    if df.empty:
        st.warning("No hay datos para los filtros seleccionados o HDFS está vacío.")
        time.sleep(5)
        st.rerun()
        return

    st.success(f"✅ Datos en memoria: {len(df):,} registros")

    # PROCESAMIENTO EXTRA
    df = calculate_logistics_costs(df)
    df = calculate_efficiency_metrics(df)

    # =============================================
    # DASHBOARD COMPLETO
    # =============================================

    st.subheader("📦 Nivel de Stock")

    with st.expander("🔎 Filtros del gráfico"):
        f_cat = st.multiselect(
            "Categoría",
            df["category"].unique(),
            default=df["category"].unique(),
            key="stock_cat"
        )

        f_reg = st.multiselect(
            "Región",
            df["region"].unique(),
            default=df["region"].unique(),
            key="stock_reg"
        )

    df_plot = df[
        df["category"].isin(f_cat) &
        df["region"].isin(f_reg)
    ]

    stock_region = df_plot.groupby("region")["inventory_level"].sum().reset_index()

    fig_stock = px.bar(
        stock_region,
        x="region",
        y="inventory_level",
        title="Inventario total por región"
    )

    st.plotly_chart(fig_stock, use_container_width=True)


    # =============================================
    # 🔮 PREDICCIÓN VS REAL
    # =============================================

    st.subheader("🔮 Predicción de Demanda")

    with st.expander("🔎 Filtros del gráfico"):
        f_cat = st.multiselect(
            "Categoría",
            df["category"].unique(),
            default=df["category"].unique(),
            key="forecast_cat"
        )

        f_reg = st.multiselect(
            "Región",
            df["region"].unique(),
            default=df["region"].unique(),
            key="forecast_reg"
        )

    df_plot = df[
        df["category"].isin(f_cat) &
        df["region"].isin(f_reg)
    ]

    # --- Comparación temporal ---
    if df_plot['date'].nunique() > 1:

        daily_compare = df_plot.groupby('date').agg({
            'units_sold':'sum',
            'demand_forecast':'sum'
        }).reset_index()

        fig = px.line(
            daily_compare,
            x='date',
            y=['units_sold','demand_forecast'],
            title="Demanda Real vs Pronóstico (por Día)"
        )
        st.plotly_chart(fig, use_container_width=True)

    # --- Comparación categórica ---
    else:
        st.info("Datos de una sola fecha → Mostrando comparación por categoría")

        cat_compare = df_plot.groupby('category').agg({
            'units_sold':'sum',
            'demand_forecast':'sum'
        }).reset_index()

        fig = px.bar(
            cat_compare,
            x='category',
            y=['units_sold','demand_forecast'],
            barmode='group',
            title="Demanda Real vs Pronóstico por Categoría"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # =============================================
    # 📉 VISUALIZACIÓN DE PRECISIÓN DE PRONÓSTICO
    # =============================================

    st.subheader("📉 Precisión del Forecast")

    with st.expander("🔎 Filtros del gráfico"):
        f_cat = st.multiselect(
            "Categoría",
            df["category"].unique(),
            default=df["category"].unique(),
            key="error_cat"
        )

        f_reg = st.multiselect(
            "Región",
            df["region"].unique(),
            default=df["region"].unique(),
            key="error_reg"
        )

    df_plot = df[
        df["category"].isin(f_cat) &
        df["region"].isin(f_reg)
    ]

    # Asegurar que las columnas existen
    if {'demand_forecast', 'units_sold', 'date'}.issubset(df.columns):

        # Error absoluto
        df_plot['forecast_error_units'] = abs(df_plot['demand_forecast'] - df_plot['units_sold'])
        
        # Porcentaje de error (MAPE-like)
        df_plot['forecast_error_pct'] = (
            df_plot['forecast_error_units'] /
            df_plot['units_sold'].replace(0, 1)
        ) * 100
        
        # MÉTRICAS 
        total_real = df_plot['units_sold'].sum()
        total_forecast = df_plot['demand_forecast'].sum()

        # Evitar división por cero
        if total_real == 0:
            avg_error_pct = 0
            avg_accuracy = 100
        else:
            avg_error_pct = abs(total_forecast - total_real) / total_real * 100
            avg_accuracy = max(0, 100 - avg_error_pct)

        avg_error_units = (
            abs(df_plot['demand_forecast'] - df_plot['units_sold'])
            .mean()
        )

        # ---- KPIs DEL FORECAST ----
        col_f1, col_f2, col_f3 = st.columns(3)

        col_f1.metric(
            "✅ Precisión Media",
            f"{avg_accuracy:.2f} %"
        )

        col_f2.metric(
            "📉 Error Medio (Unidades)",
            f"{avg_error_units:,.1f}"
        )

        col_f3.metric(
            "📊 Error Porcentual Medio",
            f"{avg_error_pct:.2f} %",
            delta="Bueno" if avg_error_pct < 25 else "Moderado" if avg_error_pct < 40 else "Alto"
        )

        # ---- TENDENCIA DIARIA DEL ERROR ----
        daily_error = df_plot.groupby('date').agg({
            'forecast_error_units': 'mean',
            'forecast_error_pct': 'mean'
        }).reset_index()

        fig_error = px.line(
            daily_error,
            x='date',
            y='forecast_error_pct',
            title="📈 Evolución diaria del Error de Forecast (%)",
            markers=True
        )

        fig_error.update_layout(
            yaxis_title="Error Porcentual (%)",
            xaxis_title="Fecha",
            hovermode="x unified"
        )

        st.plotly_chart(fig_error, use_container_width=True)

    else:
        st.warning("No hay datos suficientes para calcular el error de predicción.")


    # =============================================
    # 🚚 COSTOS LOGÍSTICOS
    # =============================================

    st.subheader("🚚 Costos logísticos")

    with st.expander("🔎 Filtros del gráfico"):
        f_cat = st.multiselect(
            "Categoría",
            df["category"].unique(),
            default=df["category"].unique(),
            key="cost_cat"
        )

        f_reg = st.multiselect(
            "Región",
            df["region"].unique(),
            default=df["region"].unique(),
            key="cost_reg"
        )

    df_plot = df[
        df["category"].isin(f_cat) &
        df["region"].isin(f_reg)
    ]

    cost_region = df_plot.groupby("region")["logistics_cost"].sum().reset_index()

    fig_cost = px.bar(
        cost_region,
        x="region",
        y="logistics_cost",
        title="Costo logístico por región"
    )

    st.plotly_chart(fig_cost, use_container_width=True)


    # =============================================
    # ⚖️ COMPARACIÓN DE EFICIENCIA
    # =============================================

    st.subheader("⚖️ Eficiencia Operativa por Región")

    with st.expander("🔎 Filtros del gráfico"):
        f_cat = st.multiselect(
            "Categoría",
            df["category"].unique(),
            default=df["category"].unique(),
            key="eff_cat"
        )

        f_reg = st.multiselect(
            "Región",
            df["region"].unique(),
            default=df["region"].unique(),
            key="eff_reg"
        )

    df_plot = df[
        df["category"].isin(f_cat) &
        df["region"].isin(f_reg)
    ]

    efficiency = df_plot.groupby("region").agg(
        turnover=("inventory_turnover", "mean"),
        pricing=("pricing_efficiency", "mean"),
        forecast_acc=("forecast_accuracy", "mean")
    ).reset_index()

    st.dataframe(efficiency.round(2), use_container_width=True)


    # =============================================
    # 🚨 ALERTAS DE STOCK BAJO
    # =============================================

    st.subheader("🚨 Alertas de Stock")

    threshold = st.slider("Umbral mínimo de stock", 5, 100, 20)

    with st.expander("🔎 Filtros del gráfico"):
        f_cat = st.multiselect(
            "Categoría",
            df["category"].unique(),
            default=df["category"].unique(),
            key="alert_cat"
        )

        f_reg = st.multiselect(
            "Región",
            df["region"].unique(),
            default=df["region"].unique(),
            key="alert_reg"
        )

    df_plot = df[
        df["category"].isin(f_cat) &
        df["region"].isin(f_reg)
    ]

    alerts = df_plot[df_plot["inventory_level"] < threshold]

    if alerts.empty:
        st.success("✅ No hay productos con stock crítico.")
    else:
        st.error(f"⚠️ {len(alerts)} productos sobrepasan el umbral crítico")
        st.dataframe(
            alerts[
                ["date", "category", "region", "inventory_level",
                "units_sold", "demand_forecast"]
            ],
            use_container_width=True
        )


    # =============================================
    # 🗺️ MAPA DE RUTAS LOGISTICAS ACTIVAS
    # =============================================

    st.subheader("🗺️ Rutas Logísticas Activas")

    with st.expander("🔎 Filtros del gráfico"):
        f_reg = st.multiselect(
            "Región",
            df["region"].unique(),
            default=df["region"].unique(),
            key="map_reg"
        )

    df_plot = df[df["region"].isin(f_reg)]

    # Coordenadas simuladas por región
    REGION_COORDS = {
        "North": (45, -100),
        "South": (30, -95),
        "East": (40, -75),
        "West": (37, -120),
        "Central": (39, -98)
    }

    routes = []

    for reg, coords in REGION_COORDS.items():
        if reg in df_plot["region"].unique():
            routes.append({
                "region": reg,
                "lat": coords[0],
                "lon": coords[1],
                "volume": df_plot[df_plot["region"] == reg]["units_sold"].sum()
            })

    map_df = pd.DataFrame(routes)

    fig_map = px.scatter_geo(
        map_df,
        lat="lat",
        lon="lon",
        size="volume",
        hover_name="region",
        title="Actividad Logística por Región",
        projection="natural earth"
    )

    st.plotly_chart(fig_map, use_container_width=True)


    # Tabla Raw (Limitada a 100 filas para no tumbar el navegador)
    with st.expander("Ver Datos Crudos (Muestra 100)"):
        st.dataframe(df_plot.head(100), use_container_width=True)

    if auto_refresh:
        time.sleep(60)
        st.rerun()

if __name__ == "__main__":
    main()