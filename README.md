# Pipeline de Ingesta y Procesamiento de Engagement en Tiempo Real

Este proyecto implementa una arquitectura completa de ingeniería de datos para rascar, procesar y visualizar las métricas de engagement de los videos en tendencia de YouTube en tiempo real.

## Arquitectura del Sistema

El flujo de datos se compone de los siguientes servicios interconectados mediante Docker:

1. **Extractor (Producer):** Script en Python (`yt-dlp`) que extrae metadatos de videos virales y los envía a Apache Kafka cada 60 segundos.
2. **Apache Kafka (Broker):** El sistema de mensajería distribuida que actúa como buffer receptivo recibiendo los datos crudos en el topic `trending_raw`.
3. **Spark Streaming (Consumer):** Consume el flujo continuo de Kafka, realiza transformaciones de limpieza, calcula un `engagement_score` en tiempo real y realiza agrupaciones por ventanas de tiempo.
4. **Redis (Base de datos en memoria):** Almacena el ranking acumulado actualizado dinámicamente mediante estructuras de datos ordenadas (`ZADD`).
5. **Dashboard:** Aplicación web (Streamlit) encargada de leer el ranking de Redis y mostrar el **Top 10 - Picos de Viralidad Inmediata**.

---

## Requisitos Previos

Antes de encender la máquina, asegúrate de tener instalado en tu sistema:

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

---

## Instrucciones de Encendido (Paso a Paso)

Sigue estos comandos en tu terminal ubicándote en la raíz del proyecto para inicializar todo el entorno de forma correcta:

### 1. Construir las imágenes de Docker

Es fundamental construir los contenedores para asegurar que todas las dependencias del sistema operativo (como `ffmpeg` y `nodejs` en el extractor, o el `JRE` en Spark) y las librerías de Python se instalen correctamente.

```bash
docker-compose build

```

### 2. Levantar los servicios

Ejecuta el siguiente comando para iniciar todos los componentes del pipeline en segundo plano (`detached mode`):

```bash
docker-compose up -d

```

### 3. Verificar que todo esté corriendo

Puedes revisar el estado de los contenedores usando:

```bash
docker-compose ps

```

---

## ¡IMPORTANTE! El Regla de los 5 Minutos

Al abrir el dashboard por primera vez en tu navegador (**`http://localhost:8501`**), es posible que la gráfica aparezca en blanco o incompleta. **Esto es completamente normal y esperado debido a la lógica de procesamiento diseñada:**

- **Frecuencia del Extractor:** El extractor realiza consultas espaciadas cada **60 segundos** para evitar bloqueos por rate-limiting de YouTube.
- **Ventanas de Tiempo en Spark:** El motor de Spark Streaming agrupa los streams de datos mediante ventanas de tiempo de **1 minuto**.
- **Mecanismo de Watermark:** Para manejar datos tardíos y evitar inconsistencias en el procesamiento en tiempo real, se configuró una marca de agua (`withWatermark`) de **2 minutos**.
- **Persistencia en Redis:** Spark procesa micro-batches y computa el estado agregando el puntaje máximo (`max("engagement_score")`) antes de realizar la escritura por lote en la base de datos distribuida.

> **Nota para el equipo:** Debemos esperar **mínimo 5 minutos** tras el encendido inicial para que el pipeline estabilice su flujo continuo, Kafka distribuya las particiones, Spark complete sus ciclos de micro-batches con watermarking y Redis acumule suficientes registros históricos para renderizar un **Top 10 consistente y real** en la pantalla.

---

## Apagar el Sistema

Cuando termines de realizar las pruebas o la presentación, detén y limpia los recursos del contenedor ejecutando:

```bash
docker-compose down
```
