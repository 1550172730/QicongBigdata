from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

def read_kafka(env):
    source = KafkaSource \
            .builder() \
            .set_bootstrap_servers('b-2-public.flinkcluster.1wl9zp.c3.kafka.us-east-2.amazonaws.com:9196,b-1-public.flinkcluster.1wl9zp.c3.kafka.us-east-2.amazonaws.com:9196') \
            .set_group_id('flink-group-1') \
            .set_topics('flink-topic') \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
            .set_property('security.protocol', 'SASL_SSL') \
            .set_property('sasl.mechanism', 'SCRAM-SHA-512') \
            .set_property('sasl.jaas.config', 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="lqicongl" password="tqw961110";') \
            .set_property('ssl.endpoint.identification.algorithm', 'https') \
            .build()

    kafka_source = env.from_source(
        source=source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name='KafkaSource'
    )

    kafka_source.print()

if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # 添加Kafka连接器JAR包
    kafka_jar = "file:///C:/Users/Administrator/PycharmProjects/QicongBigdata/jar/flink-sql-connector-kafka-3.1.0-1.18.jar"
    env.add_jars(kafka_jar)

    read_kafka(env)
    env.execute("Kafka Demo")