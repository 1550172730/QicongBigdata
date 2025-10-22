from kafka import KafkaConsumer
import json

def main():
    consumer = KafkaConsumer(
        'flink-topic',
        bootstrap_servers=['b-2-public.flinkcluster.1wl9zp.c3.kafka.us-east-2.amazonaws.com:9196',
                          'b-1-public.flinkcluster.1wl9zp.c3.kafka.us-east-2.amazonaws.com:9196'],
        group_id='python-consumer-group',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: x.decode('utf-8'),
        security_protocol='SASL_SSL',
        sasl_mechanism='SCRAM-SHA-512',
        sasl_plain_username='lqicongl',
        sasl_plain_password='tqw961110',
        ssl_check_hostname=True
    )
    
    print("开始监听Kafka数据...")
    
    for message in consumer:
        print(f"收到消息: {message.value}")

if __name__ == "__main__":
    main()