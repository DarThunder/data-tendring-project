import streamlit as st
import redis
import pandas as pd
import psycopg2
import time
from datetime import timedelta

st.set_page_config(page_title="Trending Dashboard", layout="wide")
st.title("Pipeline de Engagement")

reporte = st.sidebar.selectbox(
    "Reportes",
    [
        "Top Videos",
        "Engagement Promedio",
        "Videos Procesados",
        "Comparativo Semanal"
    ]
)

@st.cache_resource
def get_redis_connection():
    return redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

def get_postgres_connection():
    try:
        return psycopg2.connect(
            host="postgres",
            port=5432,
            dbname="viral_history",
            user="admin",
            password="adminpassword"
        )
    except Exception as e:
        st.error(f"Error conectando a DB Histórica: {e}")
        return None

tab1, tab2 = st.tabs(["Tiempo Real (Redis)", "Análisis Histórico (Postgres)"])

with tab1:
    st.markdown("Monitoreo de los videos más virales al momento.")
    r = get_redis_connection()
    placeholder = st.empty()
    
    if st.checkbox("Actualizar Tiempo Real Automáticamente", value=True):
        top_videos = r.zrevrange("ranking_viralidad", 0, 9, withscores=True)
        with placeholder.container():
            if top_videos:
                df = pd.DataFrame(top_videos, columns=["Video", "Engagement Score"])
                st.subheader("Top 10 - Picos de Viralidad Inmediata")
                st.bar_chart(data=df.set_index("Video"))
                st.dataframe(df, use_container_width=True)
            else:
                st.info("Esperando a que Spark Streaming procese el primer micro-batch...")
        time.sleep(2)

with tab2:
    st.markdown("Filtra y analiza los eventos históricos guardados.")
    conn = get_postgres_connection()
    
    if conn:
        try:
            df_hist = pd.read_sql("SELECT * FROM historical_events", conn)
            
            if not df_hist.empty:
                df_hist['upload_date'] = pd.to_datetime(df_hist['upload_date'], format='mixed', errors='coerce').dt.date
                
                fecha_max = df_hist['upload_date'].max()
                
                st.markdown("### Los más virales")
                col_dia, col_sem, col_mes = st.columns(3)
                
                df_dia = df_hist[df_hist['upload_date'] == fecha_max]
                if not df_dia.empty:
                    top_dia = df_dia.loc[df_dia['view_count'].idxmax()]
                    col_dia.metric(label=f"Top del Día ({fecha_max})", value=top_dia['title'][:30]+"...", delta=f"{int(top_dia['view_count']):,} vistas")

                fecha_semana = fecha_max - timedelta(days=7)
                df_semana = df_hist[df_hist['upload_date'] >= fecha_semana]
                if not df_semana.empty:
                    top_semana = df_semana.loc[df_semana['view_count'].idxmax()]
                    col_sem.metric(label="Top de la Semana", value=top_semana['title'][:30]+"...", delta=f"{int(top_semana['view_count']):,} vistas")

                fecha_mes = fecha_max - timedelta(days=30)
                df_mes = df_hist[df_hist['upload_date'] >= fecha_mes]
                if not df_mes.empty:
                    top_mes = df_mes.loc[df_mes['view_count'].idxmax()]
                    col_mes.metric(label="Top del Mes", value=top_mes['title'][:30]+"...", delta=f"{int(top_mes['view_count']):,} vistas")

                st.divider()

                st.markdown("## Reportes")

                if reporte == "Top Videos":

                    top_videos = (
                        df_hist.groupby("title")["view_count"]
                        .max()
                        .reset_index()
                        .sort_values(
                            by="view_count",
                            ascending=False
                        )
                        .head(10)
                    )

                    st.subheader("Top 10 Videos Más Vistos")

                    st.bar_chart(
                        top_videos.set_index("title")
                    )

                elif reporte == "Engagement Promedio":

                    df_hist["engagement_score"] = (
                        df_hist["view_count"]
                        + (df_hist["like_count"] * 5)
                        + (df_hist["comment_count"] * 10)
                    )

                    promedio = df_hist["engagement_score"].mean()

                    st.metric(
                        "Engagement Promedio",
                        f"{promedio:,.0f}"
                    )

                elif reporte == "Videos Procesados":

                    st.metric(
                        "Videos Procesados",
                        len(df_hist)
                    )

                elif reporte == "Comparativo Semanal":

                    df_hist["upload_date"] = pd.to_datetime(
                        df_hist["upload_date"]
                    )

                    semanal = (
                        df_hist.groupby(
                            pd.Grouper(
                                key="upload_date",
                                freq="W"
                            )
                        )["view_count"]
                        .sum()
                        .reset_index()
                    )

                    st.subheader(
                        "Comparativo Semanal de Vistas"
                    )

                    st.line_chart(
                        semanal.set_index("upload_date")
                    )

                st.divider()

                st.markdown("### Explorador de Histórico")
                col1, col2 = st.columns(2)
                
                with col1:
                    min_date = df_hist['upload_date'].min()
                    rango_fechas = st.date_input("Filtrar por Rango de Fechas", [min_date, fecha_max], min_value=min_date, max_value=fecha_max)
                
                with col2:
                    filtro_plat = st.multiselect("Filtrar por Plataforma", options=df_hist['platform'].unique(), default=df_hist['platform'].unique())
                
                df_filtrado = df_hist.copy()
                
                if len(rango_fechas) == 2:
                    df_filtrado = df_filtrado[(df_filtrado['upload_date'] >= rango_fechas[0]) & (df_filtrado['upload_date'] <= rango_fechas[1])]
                    
                if filtro_plat:
                    df_filtrado = df_filtrado[df_filtrado['platform'].isin(filtro_plat)]
                
                columnas_mostrar = ['upload_date', 'title', 'platform', 'view_count', 'like_count', 'comment_count']
                df_mostrar = df_filtrado[columnas_mostrar].sort_values(by='view_count', ascending=False)
                
                df_mostrar.columns = ['Fecha', 'Título del Video', 'Plataforma', 'Vistas', 'Likes', 'Comentarios']
                
                st.dataframe(df_mostrar, use_container_width=True, hide_index=True)

            else:
                st.info("Aún no hay datos históricos registrados en la base de datos.")
        except Exception as e:
            st.warning(f"La tabla de eventos históricos aún no ha sido creada o hubo un error: {e}")