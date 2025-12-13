from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

USERS = list(range(1, 101))
PRODUCTS = list(range(100, 120))
CATEGORIES = ['Tech', 'Home', 'Sports', 'Fashion']
CITIES = ['Guayaquil', 'Quito', 'Cuenca']
PROVINCES = ['Guayas', 'Pichincha', 'Azuay']

def generate_event():
    event_type = random.choices(
        ['page_view', 'add_to_cart', 'purchase'],
        weights=[0.6, 0.25, 0.15]
    )[0]

    event = {
        "user_id": random.choice(USERS),
        "product_id": random.choice(PRODUCTS),
        "category": random.choice(CATEGORIES),
        "event_type": event_type,
        "price": round(random.uniform(10, 500), 2),
        "timestamp": datetime.utcnow().isoformat(),
        "store_id": "ST005",
        "store_type": "physical_store",
        "city": random.choice(CITIES),
        "province": random.choice(PROVINCES),
        "location": {
            "lat": -2.1708,
            "lon": -79.9224
        }
    }
    return event

def topic_for_event(event_type):
    if event_type == 'page_view':
        return 'page_views'
    elif event_type == 'add_to_cart':
        return 'cart_events'
    else:
        return 'orders'

print("Productor de eventos e-commerce iniciado...")

while True:
    event = generate_event()
    topic = topic_for_event(event['event_type'])

    producer.send(topic, event)
    print(f"Evento enviado a {topic}: {event}")

    time.sleep(1)
