from kafka import KafkaConsumer

c = KafkaConsumer(
    'financial-stream',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    group_id='debug-check',
    consumer_timeout_ms=5000,
)

for i, msg in enumerate(c):
    print(type(msg.value), msg.value)
    if i >= 2:
        break
