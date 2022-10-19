package org.joisen.mapreduce.outputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author : joisen
 * @date : 20:27 2022/9/9
 */
public class LogReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // 防止有相同数据出现数据丢失的情况
        for (NullWritable value : values) {
            context.write(key, NullWritable.get());
        }


    }
}
