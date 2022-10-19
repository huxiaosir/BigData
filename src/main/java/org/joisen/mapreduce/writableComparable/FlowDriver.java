package org.joisen.mapreduce.writableComparable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author : joisen
 * @date : 18:33 2022/9/8
 */
public class FlowDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 2 设置jar
        job.setJarByClass(FlowDriver.class);
        // 3 关联 Mapper 和 Reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        // 4 设置mapper输出的key和value类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        // 5 设置数据最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        // 6 设置数据的输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\BigDATA\\mapreducefile\\file_out\\outputflow"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\BigDATA\\mapreducefile\\file_out\\outputflow3"));
        // 7 提交job  true打印的日志信息更多
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


}
