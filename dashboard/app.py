import streamlit as st
import redis
import pandas as pd
import time

st.set_page_config(page_title="Trending Dashboard", layout="wide")
st.title("🚀 Pipeline de Engagement en Tiempo Real")
st.markdown("Monitoreo de los videos más virales de YouTube.")

@st.cache_resource
def get_redis_connection():
    return redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

r = get_redis_connection()

placeholder = st.empty()

while True:
    top_videos = r.zrevrange("ranking_viralidad", 0, 9, withscores=True)
    
    with placeholder.container():
        if top_videos:
            df = pd.DataFrame(top_videos, columns=["Video", "Engagement Score"])
            
            st.subheader("Top 10 - Picos de Viralidad Inmediata")

            st.bar_chart(data=df.set_index("Video"))

            st.dataframe(df, use_container_width=True)
        else:
            st.info("Esperando a que Spark Streaming procese el primer micro-batch...")
    
    time.sleep(5)