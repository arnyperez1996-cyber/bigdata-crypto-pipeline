import time
import json
import requests
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'crypto_prices'
url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,cardano&vs_currencies=usd"

print("Iniciando captura de datos desde la API pública hacia kafka...")

try:
    while True:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            payload = {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "bitcoin_usd": data["bitcoin"]["usd"],
                "ethereum_usd": data["ethereum"]["usd"],
                "cardano_usd": data["cardano"]["usd"]
            }
            producer.send(topic_name, value=payload)
            print(f"Evento enviado a Kafka: {payload}")
        else:
            print("Error al consultar la API")
        
        time.sleep(5)
except KeyboardInterrupt:
    print("Productor detenido.")
