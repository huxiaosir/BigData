package org.joisen.zookeeper.case2;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * @author : joisen
 * @date : 16:30 2022/9/21
 */
public class DistributedLockTest {

    public static void main(String[] args) throws InterruptedException, IOException, KeeperException {
        final DistributedLock lock1 = new DistributedLock();
        final DistributedLock lock2 = new DistributedLock();

        new Thread(new Runnable() {
            @Override
            public void run() {
                // 加锁
                try {
                    lock1.zkLock();
                    System.out.println("线程1 启动，获取到锁...");
                    Thread.sleep(5*1000);
                    lock1.zkUnLock();
                    System.out.println("线程1 释放锁...");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }).start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                // 加锁
                try {
                    lock2.zkLock();
                    System.out.println("线程2 启动，获取到锁...");
                    Thread.sleep(5*1000);
                    lock2.zkUnLock();
                    System.out.println("线程2 释放锁...");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }).start();


    }

}
