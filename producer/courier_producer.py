import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Configuración del Producer de Kafka (Usando el tópico del docker-compose)
bootstrap_servers = ['localhost:9092']
topic_name = 'driver_security_events'

producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

# Lista de repartidores de prueba
couriers = ["DEL-101", "DEL-102", "DEL-103"]

print(f"Iniciando Producer (Umbral velocidad: > 45 km/h, Batería crítica: < 15%). Tópico: {topic_name}")

try:
    while True:
        for courier_id in couriers:
            payload = {
                "courier_id": courier_id,
                "latitude": round(4.6097 + random.uniform(-0.01, 0.01), 6),
                "longitude": round(-74.0817 + random.uniform(-0.01, 0.01), 6),
                "speed": round(random.uniform(10.0, 75.0), 2),       # Alerta si > 45
                "battery_level": random.randint(5, 100),            # Alerta si < 15
                "status": random.choice(["in_transit", "stopped", "delivering"]),
                "timestamp": datetime.utcnow().isoformat() + "Z"
            }

            # courier_id como partition key para mantener el orden
            producer.send(topic_name, key=courier_id, value=payload)
            print(f"Enviado a [{topic_name}] -> {payload}")
            
        time.sleep(2)

except KeyboardInterrupt:
    print("\n Producer detenido por el usuario.")
finally:
    producer.close()