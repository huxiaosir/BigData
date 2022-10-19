package org.joisen.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author : joisen
 * @date : 12:22 2022/10/13
 */
public class CustomProducerSync {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //0.配置
        Properties properties = new Properties();
        // 连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
        // 指定对应德key和value德序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //1.创建kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //2.
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("first","joisen"+i)).get();
        }

        //3 关闭资源
        kafkaProducer.close();

    }
}
