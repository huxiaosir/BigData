package org.joisen.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author : joisen
 * @date : 16:50 2022/9/7
 */
/**
 * public class Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *     KEYIN：reduce 阶段输入的key的类型(word的偏移量)：Text
 *     VALUEIN：reduce 阶段输入的value类型：IntWritable
 *     KEYOUT：reduce 阶段输出的key类型：Text
 *     VALUEOUT：reduce 阶段输出的value类型：IntWritable
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable outV = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        // hadoop, (1,1)
        // 累加
        for (IntWritable value : values) {
            sum += value.get();
        }

        outV.set(sum);
        // 写出
        context.write(key, outV);

    }
}
