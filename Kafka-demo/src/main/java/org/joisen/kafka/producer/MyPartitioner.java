package org.joisen.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author : joisen
 * @date : 20:27 2022/10/13
 */
public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        //获取数据
        String msgValues = value.toString();
        int partition;
        if (msgValues.contains("joisen")){
            partition = 0;
        }else {
            partition = 1;
        }

        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
