import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings
warnings.filterwarnings('ignore')

# Configuración de la página
st.set_page_config(
    page_title="Retail Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

def get_postgres_connection():
    """Establece conexión con PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host="postgres",
            database="hive",
            user="hive",
            password="hive",
            port="5432"
        )
        return conn
    except Exception as e:
        st.error(f"❌ Error conectando a PostgreSQL: {e}")
        return None

def execute_query(query, params=None):
    """Ejecuta consulta y retorna DataFrame"""
    conn = get_postgres_connection()
    if conn:
        try:
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            return df
        except Exception as e:
            st.error(f"❌ Error en consulta: {e}")
            conn.close()
            return pd.DataFrame()
    return pd.DataFrame()

def load_data():
    """Carga datos principales con cache"""
    query = """
    SELECT 
        date,
        store_id,
        product_id,
        category,
        region,
        inventory_level,
        units_sold,
        units_ordered,
        demand_forecast,
        price,
        discount,
        weather_condition,
        holiday_promotion,
        competitor_pricing,
        seasonality,
        (units_sold * price) as revenue,
        (units_sold * price * discount) as discount_amount,
        -- Cálculo de precisión de demanda CORREGIDO para PostgreSQL
        CASE 
            WHEN demand_forecast > 0 THEN 
                CAST((1 - ABS(units_sold - demand_forecast) / demand_forecast) * 100 AS DECIMAL(10,2))
            ELSE 0 
        END as forecast_accuracy
    FROM retail_sales 
    ORDER BY date DESC
    """
    return execute_query(query)

def calculate_logistics_costs(df):
    """
    Simula costos logísticos basados en:
    - Distancia por región (costo fijo)
    - Volumen de inventario (costo variable)
    - Tipo de producto (costo categoría)
    """
    # Costos base por región (simulados)
    region_costs = {
        'North': 1.2, 'South': 1.0, 'East': 1.3, 
        'West': 1.4, 'Central': 1.1, 'Northeast': 1.5, 'Southwest': 1.2
    }
    
    # Costos por categoría (simulados)
    category_costs = {
        'Electronics': 1.8, 'Groceries': 1.0, 'Clothing': 1.2,
        'Furniture': 2.0, 'Toys': 1.3, 'Sports': 1.4, 'Books': 1.1
    }
    
    # Calcular costos logísticos simulados
    df['base_logistics_cost'] = df['region'].map(region_costs).fillna(1.2)
    df['category_cost_multiplier'] = df['category'].map(category_costs).fillna(1.2)
    df['logistics_cost'] = (
        df['base_logistics_cost'] * 
        df['category_cost_multiplier'] * 
        df['inventory_level'] * 0.1  # Costo por unidad de inventario
    )
    
    return df

def calculate_efficiency_metrics(df):
    """Calcula métricas de eficiencia"""
    # Eficiencia de inventario
    df['inventory_turnover'] = df['units_sold'] / df['inventory_level'].replace(0, 1)
    
    # Eficiencia de precio vs competencia
    df['pricing_efficiency'] = (
        (df['price'] - df['competitor_pricing']) / df['competitor_pricing'].replace(0, 1) * 100
    )
    
    # Eficiencia de promociones
    df['promotion_efficiency'] = df['units_sold'] * df['holiday_promotion']
    
    return df

def main():
    # Header principal
    st.title("🏪 Retail Analytics Dashboard")
    st.markdown("Análisis en tiempo real de ventas minoristas - PostgreSQL")
    
    # Sidebar para filtros
    st.sidebar.title("🔧 Filtros")
    
    # Cargar datos
    with st.spinner("🔄 Cargando datos desde PostgreSQL..."):
        df = load_data()
    
    if df.empty:
        st.warning("📭 No hay datos disponibles. Ejecuta el Spark Consumer primero.")
        return
    
    # Convertir la columna date a datetime
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    # Aplicar cálculos de costos logísticos y eficiencia
    df = calculate_logistics_costs(df)
    df = calculate_efficiency_metrics(df)
    
    # Filtros en sidebar
    st.sidebar.subheader("Filtrar Datos")
    
    # Filtro por categoría
    categories = ['Todos'] + sorted(df['category'].dropna().unique().tolist())
    selected_category = st.sidebar.selectbox("Categoría", categories)
    
    # Filtro por región
    regions = ['Todas'] + sorted(df['region'].dropna().unique().tolist())
    selected_region = st.sidebar.selectbox("Región", regions)
    
    # Filtro por fecha
    if not df.empty:
        min_date = df['date'].min().date()
        max_date = df['date'].max().date()
    else:
        min_date = pd.Timestamp.now().date()
        max_date = pd.Timestamp.now().date()
    
    date_range = st.sidebar.date_input(
        "Rango de Fechas",
        [min_date, max_date],
        min_value=min_date,
        max_value=max_date
    )
    
    # Aplicar filtros
    filtered_df = df.copy()
    
    if selected_category != 'Todos':
        filtered_df = filtered_df[filtered_df['category'] == selected_category]
    
    if selected_region != 'Todas':
        filtered_df = filtered_df[filtered_df['region'] == selected_region]
    
    # Aplicar filtro de fecha
    if len(date_range) == 2:
        start_date, end_date = date_range
        start_date = pd.to_datetime(start_date)
        end_date = pd.to_datetime(end_date)
        
        filtered_df = filtered_df[
            (filtered_df['date'] >= start_date) & 
            (filtered_df['date'] <= end_date)
        ]
    
    # =============================================
    # 🚨 NUEVA SECCIÓN: ALERTAS DE STOCK BAJO
    # =============================================
    st.subheader("🚨 Alertas y Monitoreo")
    
    # Configurar umbral de stock bajo
    stock_threshold = st.slider(
        "Umbral de alerta de stock bajo", 
        min_value=0, 
        max_value=50, 
        value=10,
        help="Nivel de inventario mínimo para generar alertas"
    )
    
    # Identificar productos con stock bajo
    low_stock_items = filtered_df[filtered_df['inventory_level'] < stock_threshold]
    
    col_alert1, col_alert2, col_alert3 = st.columns(3)
    
    with col_alert1:
        total_low_stock = len(low_stock_items)
        st.metric(
            "Productos con Stock Bajo", 
            total_low_stock,
            delta=f"Umbral: {stock_threshold}" if total_low_stock > 0 else "Todo OK",
            delta_color="inverse" if total_low_stock > 0 else "normal"
        )
    
    with col_alert2:
        # Alertas de demanda vs inventario
        high_demand_low_stock = filtered_df[
            (filtered_df['demand_forecast'] > filtered_df['inventory_level']) & 
            (filtered_df['inventory_level'] < stock_threshold * 2)
        ]
        st.metric(
            "Riesgo de Desabastecimiento", 
            len(high_demand_low_stock),
            help="Productos con alta demanda pronosticada y bajo inventario"
        )
    
    with col_alert3:
        # Eficiencia de pronósticos
        avg_accuracy = filtered_df['forecast_accuracy'].mean()
        st.metric(
            "Precisión de Pronósticos", 
            f"{avg_accuracy:.1f}%",
            delta="Alta" if avg_accuracy > 80 else "Media" if avg_accuracy > 60 else "Baja",
            delta_color="normal" if avg_accuracy > 80 else "off"
        )
    
    # Mostrar tabla de alertas detalladas
    if not low_stock_items.empty:
        with st.expander("📋 Detalle de Alertas de Stock Bajo", expanded=False):
            alert_cols = ['product_id', 'category', 'region', 'inventory_level', 'demand_forecast', 'units_sold']
            st.dataframe(
                low_stock_items[alert_cols].sort_values('inventory_level').head(10),
                use_container_width=True
            )
    
    # =============================================
    # 📈 MÉTRICAS PRINCIPALES (MEJORADAS)
    # =============================================
    st.subheader("📈 Métricas Clave")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        total_revenue = filtered_df['revenue'].sum()
        st.metric("Ingreso Total", f"${total_revenue:,.2f}")
    
    with col2:
        total_units = filtered_df['units_sold'].sum()
        st.metric("Unidades Vendidas", f"{total_units:,.0f}")
    
    with col3:
        # NUEVO: Costos logísticos totales
        total_logistics = filtered_df['logistics_cost'].sum()
        st.metric("Costos Logísticos", f"${total_logistics:,.2f}")
    
    with col4:
        avg_inventory = filtered_df['inventory_level'].mean()
        st.metric("Inventario Promedio", f"{avg_inventory:.1f}")
    
    with col5:
        # NUEVO: Eficiencia general
        avg_turnover = filtered_df['inventory_turnover'].mean()
        st.metric("Rotación de Inventario", f"{avg_turnover:.2f}")
    
    # =============================================
    # 🗺️ NUEVA SECCIÓN: MAPAS Y RUTAS (SIMULADO)
    # =============================================
    st.subheader("🗺️ Análisis Geográfico y Logístico")
    
    col_map1, col_map2 = st.columns(2)
    
    with col_map1:
        # Mapa de calor por región (simulado)
        st.markdown("**📊 Actividad por Región**")
        region_activity = filtered_df.groupby('region').agg({
            'revenue': 'sum',
            'units_sold': 'sum',
            'logistics_cost': 'sum'
        }).reset_index()
        
        if not region_activity.empty:
            fig_region = px.bar(
                region_activity,
                x='region',
                y=['revenue', 'logistics_cost'],
                title="Ingresos vs Costos Logísticos por Región",
                barmode='group'
            )
            st.plotly_chart(fig_region, use_container_width=True)
    
    with col_map2:
        # Eficiencia logística por región
        st.markdown("**📦 Eficiencia Logística**")
        region_efficiency = filtered_df.groupby('region').agg({
            'logistics_cost': 'sum',
            'units_sold': 'sum',
            'inventory_turnover': 'mean'
        }).reset_index()
        
        region_efficiency['cost_per_unit'] = (
            region_efficiency['logistics_cost'] / region_efficiency['units_sold'].replace(0, 1)
        )
        
        if not region_efficiency.empty:
            fig_efficiency = px.scatter(
                region_efficiency,
                x='cost_per_unit',
                y='inventory_turnover',
                size='units_sold',
                color='region',
                title="Eficiencia: Costo vs Rotación por Región",
                hover_name='region'
            )
            st.plotly_chart(fig_efficiency, use_container_width=True)
    
    # =============================================
    # ⚡ NUEVA SECCIÓN: COMPARACIÓN DE EFICIENCIA
    # =============================================
    st.subheader("⚡ Análisis de Eficiencia Comparada")
    
    col_eff1, col_eff2 = st.columns(2)
    
    with col_eff1:
        # Eficiencia por categoría
        category_efficiency = filtered_df.groupby('category').agg({
            'inventory_turnover': 'mean',
            'forecast_accuracy': 'mean',
            'pricing_efficiency': 'mean',
            'revenue': 'sum'
        }).reset_index()
        
        if not category_efficiency.empty:
            fig_category_eff = px.bar(
                category_efficiency,
                x='category',
                y='inventory_turnover',
                title="Rotación de Inventario por Categoría",
                color='inventory_turnover',
                color_continuous_scale='viridis'
            )
            st.plotly_chart(fig_category_eff, use_container_width=True)
    
    with col_eff2:
        # Comparación de eficiencia temporal
        if not filtered_df.empty and 'date' in filtered_df.columns:
            daily_efficiency = filtered_df.groupby('date').agg({
                'inventory_turnover': 'mean',
                'forecast_accuracy': 'mean',
                'logistics_cost': 'sum'
            }).reset_index()
            
            if not daily_efficiency.empty:
                fig_trend_eff = go.Figure()
                fig_trend_eff.add_trace(go.Scatter(
                    x=daily_efficiency['date'], 
                    y=daily_efficiency['inventory_turnover'],
                    name='Rotación Inventario',
                    line=dict(color='blue')
                ))
                fig_trend_eff.add_trace(go.Scatter(
                    x=daily_efficiency['date'], 
                    y=daily_efficiency['forecast_accuracy'] / 100,
                    name='Precisión Pronósticos (escala 0-1)',
                    line=dict(color='green', dash='dash')
                ))
                fig_trend_eff.update_layout(title="Tendencia de Eficiencia Diaria")
                st.plotly_chart(fig_trend_eff, use_container_width=True)
    
    # =============================================
    # 📊 GRÁFICAS EXISTENTES (MANTENIDAS)
    # =============================================
    st.subheader("📊 Análisis Visual Tradicional")
    
    # Primera fila de gráficas (existente)
    col1, col2 = st.columns(2)
    
    with col1:
        if not filtered_df.empty and 'category' in filtered_df.columns:
            category_sales = filtered_df.groupby('category')['revenue'].sum().reset_index()
            if not category_sales.empty:
                fig1 = px.bar(
                    category_sales, 
                    x='category', 
                    y='revenue',
                    title="Ingresos por Categoría",
                    color='revenue',
                    color_continuous_scale='viridis'
                )
                fig1.update_layout(xaxis_title="Categoría", yaxis_title="Ingresos ($)")
                st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        if not filtered_df.empty and 'date' in filtered_df.columns:
            daily_sales = filtered_df.groupby('date')['units_sold'].sum().reset_index()
            if not daily_sales.empty:
                fig2 = px.line(
                    daily_sales,
                    x='date',
                    y='units_sold',
                    title="Tendencia de Ventas Diarias",
                    line_shape='spline'
                )
                fig2.update_layout(xaxis_title="Fecha", yaxis_title="Unidades Vendidas")
                st.plotly_chart(fig2, use_container_width=True)
    
    # =============================================
    # 🔍 ANÁLISIS DETALLADO (MEJORADO)
    # =============================================
    st.subheader("🔍 Análisis Detallado y Predictivo")
    
    col_anal1, col_anal2 = st.columns(2)
    
    with col_anal1:
        # Demanda vs Real (mejorado)
        if not filtered_df.empty and all(col in filtered_df.columns for col in ['date', 'units_sold', 'demand_forecast']):
            demand_comparison = filtered_df.groupby('date').agg({
                'units_sold': 'sum',
                'demand_forecast': 'sum'
            }).reset_index()
            
            if not demand_comparison.empty:
                fig_demand = go.Figure()
                fig_demand.add_trace(go.Scatter(
                    x=demand_comparison['date'], 
                    y=demand_comparison['units_sold'],
                    name='Ventas Reales',
                    line=dict(color='blue')
                ))
                fig_demand.add_trace(go.Scatter(
                    x=demand_comparison['date'], 
                    y=demand_comparison['demand_forecast'],
                    name='Pronóstico',
                    line=dict(color='red', dash='dash')
                ))
                fig_demand.update_layout(title="Comparación: Demanda Real vs Pronosticada")
                st.plotly_chart(fig_demand, use_container_width=True)
    
    with col_anal2:
        # Análisis de eficiencia de promociones
        if not filtered_df.empty and 'holiday_promotion' in filtered_df.columns:
            promotion_analysis = filtered_df.groupby('holiday_promotion').agg({
                'units_sold': 'mean',
                'revenue': 'mean',
                'inventory_turnover': 'mean'
            }).reset_index()
            promotion_analysis['promotion_type'] = promotion_analysis['holiday_promotion'].map(
                {0: 'Sin Promoción', 1: 'Con Promoción'}
            )
            
            if not promotion_analysis.empty:
                fig_promo = px.bar(
                    promotion_analysis,
                    x='promotion_type',
                    y=['units_sold', 'revenue'],
                    title="Impacto de Promociones en Ventas e Ingresos",
                    barmode='group'
                )
                st.plotly_chart(fig_promo, use_container_width=True)
    
    # =============================================
    # 📋 TABLA DE DATOS (MEJORADA)
    # =============================================
    st.subheader("📋 Datos Detallados con Métricas de Eficiencia")
    
    # Selector de columnas para mostrar (mejorado)
    default_cols = ['date', 'category', 'region', 'units_sold', 'inventory_level', 
                   'demand_forecast', 'logistics_cost', 'inventory_turnover']
    available_cols = filtered_df.columns.tolist()
    selected_cols = st.multiselect(
        "Selecciona columnas para mostrar:",
        available_cols,
        default=default_cols
    )
    
    if selected_cols:
        display_df = filtered_df[selected_cols].copy()
        if 'date' in display_df.columns:
            display_df['date'] = display_df['date'].dt.strftime('%Y-%m-%d')
            
        st.dataframe(
            display_df.sort_values('date' if 'date' in display_df.columns else selected_cols[0], ascending=False),
            use_container_width=True,
            height=400
        )
    
    # Estadísticas descriptivas (mejoradas)
    st.subheader("📊 Estadísticas Descriptivas Completas")
    
    numeric_cols = filtered_df.select_dtypes(include=['float64', 'int64']).columns
    if len(numeric_cols) > 0:
        st.dataframe(filtered_df[numeric_cols].describe(), use_container_width=True)
    
    # Información del sistema (mejorada)
    with st.expander("ℹ️ Información del Sistema y Métricas"):
        st.info(f"**Fuente de datos:** PostgreSQL")
        st.info(f"**Total de registros:** {len(filtered_df):,}")
        if not filtered_df.empty and 'date' in filtered_df.columns:
            st.info(f"**Período de datos:** {filtered_df['date'].min().strftime('%Y-%m-%d')} a {filtered_df['date'].max().strftime('%Y-%m-%d')}")
        st.info(f"**Métricas calculadas:** Costos logísticos, Eficiencia, Rotación de inventario, Alertas de stock")
        st.info(f"**Última actualización:** {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()