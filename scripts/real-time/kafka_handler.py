import json
from kafka import KafkaConsumer, KafkaProducer

class KafkaHandler:
    def __init__(self , bootstrap_server: str):
        self.bootstrap_server = bootstrap_server
        
    def create_producer(self):
        return KafkaProducer(
            bootstrap_servers=self.bootstrap_server,
            value_serializer=lambda v : json.dumps(v).encode('utf-8')
        )
    def create_consumer(self , topic : str , group_id : str = None):
        return KafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_server,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )