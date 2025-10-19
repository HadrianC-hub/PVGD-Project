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
