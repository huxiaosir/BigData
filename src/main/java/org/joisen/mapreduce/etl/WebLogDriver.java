package org.joisen.mapreduce.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author : joisen
 * @date : 9:50 2022/9/11
 */
public class WebLogDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 设置输入输出路径
        args = new String[]{"D:\\BigDATA\\mapreducefile\\file_in\\inputlog", "D:\\BigDATA\\mapreducefile\\file_out\\outputlog"};
        // 获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        // 设置jar包路径
        job.setJarByClass(WebLogDriver.class);
        // 关联mapper
        job.setMapperClass(WebLogMapper.class);
        // 设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置reduceTask为0
        job.setNumReduceTasks(0);

        // 设置输入 and 输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }

}
