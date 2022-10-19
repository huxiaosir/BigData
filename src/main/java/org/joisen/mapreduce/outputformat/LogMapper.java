package org.joisen.mapreduce.outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author : joisen
 * @date : 20:22 2022/9/9
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 输出格式（http://www.baidu.com，NullWritable）
        // 不做任何处理
        context.write(value, NullWritable.get());
    }
}
