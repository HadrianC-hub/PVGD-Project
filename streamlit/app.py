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
