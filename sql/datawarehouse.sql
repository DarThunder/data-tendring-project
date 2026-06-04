CREATE TABLE IF NOT EXISTS dim_video (
    id_video SERIAL PRIMARY KEY,
    youtube_id TEXT UNIQUE,
    title TEXT,
    country TEXT,
    duration BIGINT
);

CREATE TABLE IF NOT EXISTS dim_fecha (
    id_fecha SERIAL PRIMARY KEY,
    fecha DATE UNIQUE,
    dia INTEGER,
    mes INTEGER,
    anio INTEGER
);

CREATE TABLE IF NOT EXISTS dim_plataforma (
    id_plataforma SERIAL PRIMARY KEY,
    nombre TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS fact_engagement (
    id_fact SERIAL PRIMARY KEY,

    id_video INTEGER REFERENCES dim_video(id_video),
    id_fecha INTEGER REFERENCES dim_fecha(id_fecha),
    id_plataforma INTEGER REFERENCES dim_plataforma(id_plataforma),

    view_count BIGINT,
    like_count BIGINT,
    comment_count BIGINT,
    engagement_score BIGINT,

    CONSTRAINT uq_fact
    UNIQUE(
        id_video,
        id_fecha,
        id_plataforma
    )
);