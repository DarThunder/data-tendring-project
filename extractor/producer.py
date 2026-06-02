import time
import json
from datetime import datetime
import os
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

load_dotenv()

API_KEY = os.getenv("GOOGLE_API_KEY")
REGION_CODE = "US"
MAX_RESULTS = 50

def conn():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Conexión exitosa a Apache Kafka.")
            return producer
        except NoBrokersAvailable:
            print("Esperando a que el broker de Kafka esté disponible...")
            time.sleep(5)

def crear_mensaje_estandar(video):
    snippet = video.get('snippet', {})
    stats = video.get('statistics', {})
    content_details = video.get('contentDetails', {})

    return {
        "id": video.get('id', ''),
        "title": snippet.get('title', 'Video cabronsisimo'),
        "platform": "youtube",
        "view_count": int(stats.get('viewCount', 0)),
        "like_count": int(stats.get('likeCount', 0)),
        "dislike_count": 0,
        "comment_count": int(stats.get('commentCount', 0)),
        "country": REGION_CODE,
        "locality": "Desconocida",
        "upload_date": snippet.get('publishedAt', datetime.now().strftime("%Y-%m-%d"))[:10],
        "duration": content_details.get('duration', 'PT0S'),
        "extraction_date": datetime.now().isoformat() + "Z"
    }

def extract_youtube_trending():
    print(f"Buscando videos más populares en YouTube ({REGION_CODE})...")
    mensajes = []
    
    try:
        youtube = build('youtube', 'v3', developerKey=API_KEY)
        
        request = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            chart="mostPopular",
            regionCode=REGION_CODE,
            maxResults=MAX_RESULTS
        )
        response = request.execute()
        
        for video in response.get('items', []):
            msg = crear_mensaje_estandar(video)
            mensajes.append(msg)
            
        print(f"Se obtuvieron {len(mensajes)} videos trending.")
        
    except HttpError as e:
        print(f"Error en la API de YouTube: {e}")
        if e.resp.status == 403:
            print("Posible quota excedida o API Key inválida.")
    except Exception as e:
        print(f"Error inesperado: {e}")
    
    return mensajes

def extract_send(producer):
    print(f"--- Iniciando ciclo de extracción: {datetime.now().isoformat()} ---")
    
    datos_youtube = extract_youtube_trending()
    
    for registro in datos_youtube:
        producer.send('trending_raw', value=registro)
        
    print(f"¡Éxito! Se enviaron {len(datos_youtube)} registros a Kafka.")

if __name__ == '__main__':
    productor = conn()
    
    while True:
        extract_send(productor)
        print("Esperando 60 segundos para el próximo ciclo...")
        time.sleep(60)