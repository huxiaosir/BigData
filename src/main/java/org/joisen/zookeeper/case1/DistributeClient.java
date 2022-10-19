package org.joisen.zookeeper.case1;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;

/**
 * @author : joisen
 * @date : 12:52 2022/9/21
 */
public class DistributeClient {
    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181"; // 不能有空格
    private int sessionTimeout=2000;
    private ZooKeeper zkClient = null;
    private String parentNode="/servers";
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        DistributeClient client = new DistributeClient();
        // 获取zk连接
        client.connect();
        // 监听/servers下面子节点的增加和删除
        client.getServerList();
        // 业务逻辑
        client.business();

    }

    private void connect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // 再次启动监听
                try {
                    getServerList();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
    // 获取服务器列表信息
    private void getServerList() throws KeeperException, InterruptedException {
        // 1 获取服务器子节点信息，并且对父节点进行监听
        List<String> children = zkClient.getChildren(parentNode, true);
        // 2 存储服务器信息列表
        ArrayList<String> servers = new ArrayList<>();
        // 3 遍历所有节点，获取节点中的主机名称信息
        for (String child : children) {
            byte[] data = zkClient.getData(parentNode + "/" + child, false, null);
            servers.add(new String(data));
        }

        // 4 打印服务器列表信息
        System.out.println(servers);

    }
    // 业务功能
    public void business() throws InterruptedException {
        System.out.println("Client is working...");
        Thread.sleep(Long.MAX_VALUE);
    }

}
