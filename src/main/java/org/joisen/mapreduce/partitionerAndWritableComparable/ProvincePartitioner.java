package org.joisen.mapreduce.partitionerAndWritableComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author : joisen
 * @date : 18:26 2022/9/9
 */
public class ProvincePartitioner extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        // 对手机号进行分类，并返回对应的分区
        String phone = text.toString();

        String prePhone = phone.substring(0, 3);

        int partition;
        if ("136".equals(prePhone)){
            partition = 0;
        }else if ("137".equals(prePhone)){
            partition = 1;
        }else if ("138".equals(prePhone)){
            partition = 2;
        }else if ("139".equals(prePhone)){
            partition = 3;
        }else{
            partition = 4;
        }

        return partition;
    }
}
