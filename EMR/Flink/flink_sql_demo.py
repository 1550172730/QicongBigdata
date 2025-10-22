from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    table_env = TableEnvironment.create(env_settings)
    
    # 添加Kafka连接器JAR包
    kafka_jar = "file:///C:/Users/Administrator/PycharmProjects/QicongBigdata/jar/flink-sql-connector-kafka-3.1.0-1.18.jar"
    table_env.get_config().get_configuration().set_string("pipeline.jars", kafka_jar)
    
    # 创建Kafka源表
    kafka_source_ddl = """
        CREATE TABLE kafka_source (
            product STRING,
            amount BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'flink-topic',
            'properties.bootstrap.servers' = 'b-2-public.flinkcluster.1wl9zp.c3.kafka.us-east-2.amazonaws.com:9196,b-1-public.flinkcluster.1wl9zp.c3.kafka.us-east-2.amazonaws.com:9196',
            'properties.group.id' = 'flink-sql-group',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'SCRAM-SHA-512',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="lqicongl" password="tqw961110";',
            'properties.ssl.endpoint.identification.algorithm' = 'https',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """
    
    table_env.execute_sql(kafka_source_ddl)
    
    # 查询并打印结果
    result = table_env.execute_sql("SELECT * FROM kafka_source")
    result.print()

if __name__ == '__main__':
    main()