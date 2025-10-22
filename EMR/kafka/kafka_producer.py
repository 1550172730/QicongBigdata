from kafka import KafkaProducer
import json
import time

def main():
    producer = KafkaProducer(
        bootstrap_servers=['b-2-public.flinkcluster.1wl9zp.c3.kafka.us-east-2.amazonaws.com:9196',
                          'b-1-public.flinkcluster.1wl9zp.c3.kafka.us-east-2.amazonaws.com:9196'],
        security_protocol='SASL_SSL',
        sasl_mechanism='SCRAM-SHA-512',
        sasl_plain_username='lqicongl',
        sasl_plain_password='tqw961110',
        ssl_check_hostname=True,
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        request_timeout_ms=30000,
        delivery_timeout_ms=120000,
        connections_max_idle_ms=540000
    )
    
    for i in range(10):
        data = {"product": f"product_{i}", "amount": i * 100}
        producer.send('flink-topic', data)
        print(f"发送: {data}")
        time.sleep(1)
    
    producer.flush()

if __name__ == "__main__":
    main()