package org.joisen.mapreduce.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author : joisen
 * @date : 9:43 2022/9/11
 */
public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取一行
        String line = value.toString();
        boolean result = parseLog(line, context);
        // 写出
        if (!result)
            return;
        context.write(value, NullWritable.get());
    }

    private boolean parseLog(String line, Context context) {
        // 切割
        String[] split = line.split(" ");
        // 判断日志长度是否大于11
        if (split.length > 11) {
            return true;
        }else{
            return false;
        }
    }
}
