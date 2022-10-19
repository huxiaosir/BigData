package org.joisen.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author : joisen
 * @date : 12:36 2022/10/13
 */
public class CustomProducerCallbackPartitions {
    public static void main(String[] args) throws InterruptedException {
        //0.配置
        Properties properties = new Properties();
        // 连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
        // 指定对应德key和value德序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // 关联自定义分区器
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"org.joisen.kafka.producer.MyPartitioner");


        //1.创建kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //2.
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("first",0,"","joisen" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        System.out.println("主题："+recordMetadata.topic()+"分区："+recordMetadata.partition());
                    }
                }
            });
            Thread.sleep(2);
        }

        //3 关闭资源
        kafkaProducer.close();

    }
}
