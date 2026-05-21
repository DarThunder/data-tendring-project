import time
import json
from yt_dlp import YoutubeDL
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

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

ydl_opts = {
    'skip_download': True,      
    'quiet': True,
    'extract_flat': False,
    'playlist_end': 50
}

def extract_send(producer):
    print("Iniciando extracción de metadatos de Tendencias...")
    with YoutubeDL(ydl_opts) as ydl:
        try:
            info = ydl.extract_info('https://www.youtube.com/feed/trending', download=False)
            
            if 'entries' in info:
                videos = info['entries'][:50]
                for video in videos:
                    if video:
                        producer.send('trending_raw', value=video)
                
                print(f"¡Éxito! Se enviaron {len(videos)} registros a Kafka.")
                
        except Exception as e:
            print(f"Error durante la extracción: {e}")

if __name__ == '__main__':
    productor = conn()
    
    while True:
        extract_send(productor)
        print("Esperando 60 segundos para el próximo ciclo...")
        time.sleep(60)