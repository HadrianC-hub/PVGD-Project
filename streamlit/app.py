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
