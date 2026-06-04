import psycopg2
import time

print("Esperando PostgreSQL...")
time.sleep(15)

conn = psycopg2.connect(
    host="postgres",
    port=5432,
    dbname="viral_history",
    user="admin",
    password="adminpassword"
)

cur = conn.cursor()

print("Creando dimensiones...")

cur.execute("""
INSERT INTO dim_video(
    youtube_id,
    title,
    country,
    duration
)
SELECT DISTINCT
    id,
    title,
    country,
    duration
FROM historical_events
ON CONFLICT (youtube_id) DO NOTHING;
""")

cur.execute("""
INSERT INTO dim_plataforma(nombre)
SELECT DISTINCT platform
FROM historical_events
ON CONFLICT(nombre) DO NOTHING;
""")

cur.execute("""
INSERT INTO dim_fecha(
    fecha,
    dia,
    mes,
    anio
)
SELECT DISTINCT
    upload_date::date,
    EXTRACT(DAY FROM upload_date::date),
    EXTRACT(MONTH FROM upload_date::date),
    EXTRACT(YEAR FROM upload_date::date)
FROM historical_events
WHERE upload_date IS NOT NULL
ON CONFLICT(fecha) DO NOTHING;
""")

print("Llenando fact_engagement...")

cur.execute("""
INSERT INTO fact_engagement(
    id_video,
    id_fecha,
    id_plataforma,
    view_count,
    like_count,
    comment_count,
    engagement_score
)
SELECT
    dv.id_video,
    df.id_fecha,
    dp.id_plataforma,

    h.view_count,
    h.like_count,
    h.comment_count,
    h.engagement_score

FROM historical_events h

JOIN dim_video dv
ON h.id = dv.youtube_id

JOIN dim_fecha df
ON h.upload_date::date = df.fecha

JOIN dim_plataforma dp
ON h.platform = dp.nombre

ON CONFLICT DO NOTHING;
""")

conn.commit()

cur.close()
conn.close()

print("Data Warehouse actualizado.")