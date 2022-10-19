package org.joisen.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.sound.midi.Soundbank;
import java.util.Properties;

/**
 * @author : joisen
 * @date : 12:22 2022/10/13
 */
public class CustomProducerTranctions {
    public static void main(String[] args) {
        //0.配置
        Properties properties = new Properties();
        // 连接集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
        // 指定对应德key和value德序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        // 指定事务id
        System.out.println("指定事务id");
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"1");

        //1.创建kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        /* 事务 */

        // 初始化事务
        kafkaProducer.initTransactions();
        System.out.println("initTransactions");
        // 启动事务
        System.out.println("beginTransaction");
        kafkaProducer.beginTransaction();
        try {
            //2.发送数据
            for (int i = 0; i < 5; i++) {
                System.out.println("print");
                kafkaProducer.send(new ProducerRecord<>("first","joisen"+i));
            }
            int i = 1/0;
            // 提交事务
            kafkaProducer.commitTransaction();
        }catch (Exception e){
            // 终止事务
            kafkaProducer.abortTransaction();
        }finally {
            //3 关闭资源
            kafkaProducer.close();
        }

    }
}
