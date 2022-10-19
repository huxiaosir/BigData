package org.joisen.zookeeper.case1;

/**
 * @author : joisen
 * @date : 12:38 2022/9/21
 */

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * 实现客户端对服务器动态上下线的监听
 */
public class DistributeServer {
    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181"; // 不能有空格
    private int sessionTimeout=2000;
    private ZooKeeper zkClient = null;
    private String parentNode="/servers";
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        // 1、获取zk连接
        DistributeServer server = new DistributeServer();
        server.getConnect();

        // 2、注册服务器到zk集群
        server.register(args[0]);

        // 3、启动业务逻辑(睡眠)
        server.business(args[0]);

    }

    private void business(String hostname) throws InterruptedException {
        System.out.println(hostname+" is working...");
        Thread.sleep(Long.MAX_VALUE);
    }

    // 注册
    private void register(String hostname) throws KeeperException, InterruptedException {
        String s = zkClient.create(parentNode+"/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + "is online" + s);
    }

    // 连接zk
    private void getConnect() throws IOException {

        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });

    }


}
