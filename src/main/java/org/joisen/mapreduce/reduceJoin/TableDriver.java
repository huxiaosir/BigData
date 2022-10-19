package org.joisen.mapreduce.reduceJoin;

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
 * @date : 18:51 2022/9/10
 */

/**
 * map 阶段先把两个文件分开处理：
 *      order表：处理后key为商品id，value为 订单号、数量、表标记
 *      pd表：处理后key为商品id，value为商品名称、表标记
 * reduce阶段将得到的bena对象以商品id为key进行合并得到目的数据
 */
public class TableDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(TableDriver.class);
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);

        job.setOutputKeyClass(TableBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\BigDATA\\mapreducefile\\file_in\\inputtable"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\BigDATA\\mapreducefile\\file_out\\outputtable2"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}
