package org.joisen.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author : joisen
 * @date : 18:26 2022/9/8
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        // 1 遍历集合累加值
        long totalUp = 0;
        long totalDown = 0;
        for (FlowBean value : values) {
            totalUp += value.getUpFlow();
            totalDown += value.getDownFlow();

        }
        // 2 封装outK，outV
        outV.setUpFlow(totalUp);
        outV.setDownFlow(totalDown);
        outV.setSumFlow();

        // 3 context写出
        context.write(key, outV);
    }
}
