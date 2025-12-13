from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'page_views',
    'cart_events',
    'orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='ecommerce-consumer-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Consumidor de e-commerce iniciado...")

for message in consumer:
    print(f"Topico: {message.topic}")
    print(f"Evento: {message.value}")
    print("-" * 50)
