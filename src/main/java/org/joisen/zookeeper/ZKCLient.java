package org.joisen.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author : joisen
 * @date : 10:41 2022/9/21
 */
public class ZKCLient {

    private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181"; // 不能有空格
    private int sessionTimeout=2000;
    private ZooKeeper zkClient = null;

//    初始化
    @Before
    public void init() throws IOException {

         zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
//                System.out.println("------------------------------------");
//                List<String> children = null;
//                try {
//                    children = zkClient.getChildren("/", true);
//                    for (String child : children) {
//                        System.out.println(child);
//                    }
//                } catch (KeeperException e) {
//                    e.printStackTrace();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//                System.out.println("------------------------------------");
            }
        });

    }
    // 创建子节点
    @Test
    public void create() throws KeeperException, InterruptedException {
        String nodeCreated = zkClient.create("/joisen", "study.jpg".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    }

    // 获取子节点并监听节点变化  这里的监听器使用init中创建的监听器，通过sleep方法使得主线程保持睡眠状态，但一直在监听
    @Test
    public void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", true);
//        for (String child : children) {
//            System.out.println(child);
//        }
        Thread.sleep(Long.MAX_VALUE);
    }

//    判断ZNode是否存在
    @Test
    public void exist() throws KeeperException, InterruptedException {
        Stat exists = zkClient.exists("/joisen1", false);
        System.out.println(exists == null ? "not exsit" : "exsit");
    }

}
