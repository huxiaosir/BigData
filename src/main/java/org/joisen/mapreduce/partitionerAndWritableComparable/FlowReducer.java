package org.joisen.mapreduce.partitionerAndWritableComparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author : joisen
 * @date : 18:26 2022/9/8
 */
public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    private FlowBean outV = new FlowBean();

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {

            context.write(value, key);
        }

    }
}
