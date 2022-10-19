package org.joisen;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @author : joisen
 * @date : 8:59 2022/10/18
 */
public class HBaseConnection {

    public static Connection connection = null;
    static {

//        Configuration conf = new Configuration();
//        conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        try {
            // 创建连接  使用读取本地配置文件的方式加载配置文件
            connection=ConnectionFactory.createConnection();
        }catch (IOException e){

        }

    }

    public static void colseConnect() throws IOException {
        if(connection != null){
            connection.close();
        }
    }

    // 直接使用创建好的连接，而不在main线程中单独创建
    public static void main(String[] args) throws IOException {
        System.out.println(connection);

        // 关闭连接
        HBaseConnection.colseConnect();
    }

    // 单线程使用连接
/*    public static void main(String[] args) throws IOException {
        // 1 创建连接配置对象
        Configuration conf = new Configuration();
        // 2 添加配置参数
        conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        // 3 创建连接  默认使用同步连接
        Connection connection = ConnectionFactory.createConnection();
        // 4 使用连接
        System.out.println(connection);
        // 5 关闭连接
        connection.close();
    }
*/
}
