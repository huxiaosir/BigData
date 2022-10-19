package org.joisen.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author : joisen
 * @date : 14:49 2022/10/16
 */
public class FlinkKafkaConsumer1 {

    public static void main(String[] args) throws Exception {
        // 1 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置运行环境默认并发度
        env.setParallelism(3);

        // 2 创建一个消费者
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test");

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>("first", new SimpleStringSchema(), properties);

        // 3 关联消费者和flink流
        env.addSource(flinkKafkaConsumer).print();

        // 4 执行
        env.execute();
    }

}
