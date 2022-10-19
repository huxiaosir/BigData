package org.joisen.zookeeper.case2;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author : joisen
 * @date : 15:17 2022/9/21
 */
public class DistributedLock {
    private final String connectString= "hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private final int sessionTimeout = 2000;
    private final ZooKeeper zk;
    private CountDownLatch connectLatch = new CountDownLatch(1);
    private CountDownLatch waitLatch = new CountDownLatch(1);
    private String waitPath;
    private String currentNode;

    public DistributedLock() throws IOException, InterruptedException, KeeperException {

        // 获取连接
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // connectLatch 如果连接上zk 可以释放
                if(watchedEvent.getState() == Event.KeeperState.SyncConnected){
                    connectLatch.countDown();
                }
                // waitLatch 需要释放
                if (watchedEvent.getType() == Event.EventType.NodeDeleted && watchedEvent.getPath().equals(waitPath)){
                    waitLatch.countDown();
                }


            }
        });
        // 等待zk正常连接后，程序才会往下执行
        connectLatch.await();

        // 判断根节点/locks 是否存在
        Stat stat = zk.exists("/locks", false);
        if(stat == null){
            zk.create("/locks","locks".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        }
    }
    //对zk加锁
    public void zkLock(){
        // 创建对应的临时带序号节点
        try {
            currentNode = zk.create("/locks/" + "seq_", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        // 判断创建的节点是否是序号最小的节点，如果是 则直接获取到锁，如果不是则需要监听他序号前面一个节点，等待其释放锁
            List<String> children = zk.getChildren("/locks", false);
            if(children.size() == 1) {
                return ;
            } else{
                Collections.sort(children);
                // 获取节点名称
                String thisNode = currentNode.substring("/locks/".length());
                // 通过节点名称获取该节点在集合中的位置
                int index = children.indexOf(thisNode);
                if(index == -1){
                    System.out.println("数据异常......");
                }else if(index == 0){
                    // 排名第一 可以获取锁
                    return ;
                }else{
                    // 需要监听其前一个节点的状态
                    waitPath = "/locks/" + children.get(index-1);
                    zk.getData(waitPath,true,null);
                    // 等待监听
                    waitLatch.await();
                    return ;
                }
            }

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }




    }

    // 解锁
    public void zkUnLock(){
        // 删除节点
        try {
            zk.delete(currentNode,-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }


}
