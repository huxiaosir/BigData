package org.joisen.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.checkerframework.checker.units.qual.C;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @author : joisen
 * @date : 11:10 2022/9/6
 *
 * 客户端代码常用套路：
 * 1、获取一个客户端对象
 * 2、执行相关的操作命令
 * 3、关闭资源
 * HDFS zookeeper
 */
public class HdfsClient {

    private FileSystem fs;
    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 连接集群nn地址
        URI uri = new URI("hdfs://hadoop102:8020");
        // 创建一个配置文件
        Configuration configuration = new Configuration();
        // 配置文件设置参数
        configuration.set("dfs.replication","2");
        // 用户
        String user = "joisen";

        // 获取到客户端对象
        fs = FileSystem.get(uri, configuration, user);

    }
    @After
    public void close() throws IOException {
        // 3 关闭资源
        fs.close();
    }

    // 创建目录
    @Test
    public void testMkdirs() throws IOException, URISyntaxException, InterruptedException {

        fs.mkdirs(new Path("/xiyouji/huaguoshan1"));

    }

    // 上传

    /**
     * 参数优先级： 小 -> 大         zhubajie.txt文件上传之前在resource目录下创建了hdfs-site.xml
     * hdfs-default.xml => hdfs-site.xml =>在项目资源目录下的配置文件 => configuration设置参数
     *
     * @throws IOException
     */
    @Test
    public void testPut() throws IOException {
        /**
         * 参数1：表示是否删除原数据
         * 参数2：是否允许覆盖
         * 参数3：原数据路径
         * 参数4：目的路径
         */
        fs.copyFromLocalFile(false,true, new Path("D:\\BigDATA\\zhubajie.txt"), new Path("hdfs://hadoop102/xiyouji/huaguoshan"));
    }

    // 下载
    @Test
    public void testGet() throws IOException {
        /**
         * param1：原文件是否删除（hdfs上的文件）
         * param2：原文件路径hdfs
         * param3：目标地址路径Win10
         * param4: false:会下载一个crc校验文件，用于校验文件是否出错；true：则不会
         */
        fs.copyToLocalFile(false, new Path("hdfs://hadoop102/xiyouji/huaguoshan"),
                new Path("D:\\BigDATA"),true);
    }

    // 删除
    @Test
    public void testRm() throws IOException {
        /** 删除文件
         * param1：要删除的路径
         * param2：是否递归删除
         */
//        fs.delete(new Path("/jdk-8u144-linux-x64.tar.gz"), false);

        // 删除空目录

//        fs.delete(new Path("/xiyouji"), false);
        // 删除非空目录
        fs.delete(new Path("/jinguo"),true);
    }

    // 文件更名和移动
    @Test
    public void testMv() throws IOException {
        /** 更名
         * param1：原文件路径
         * param2：目标文件路径
         */
//        fs.rename(new Path("/xiyouji/huaguoshan"), new Path("/xiyouji/meihouwang"));

        /** 文件移动和更名
         *
         */
//        fs.rename(new Path("/xiyouji/meihouwang"), new Path("/shuiliandong"));

        // 目录更名
        fs.rename(new Path("/xiyouji"), new Path("/xiyou"));

    }

    // 获取文件详细信息
    @Test
    public void fileDetail() throws IOException {
        //获取所有文件信息  param1：文件路径； param2：是否递归查询
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        // 遍历文件
        while(listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("=========="+fileStatus.getPath());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            // 获取块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));

        }

    }

    // 判断是文件夹还是文件
    @Test
    public void testFileType() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));

        for (FileStatus status : fileStatuses) {

            if (status.isFile()) {
                System.out.println("文件："+status.getPath().getName());
            }else{
                System.out.println("目录："+status.getPath().getName());
            }

        }
    }


}
