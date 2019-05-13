package com.practice.sequences;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;


//References for writing this code snippet for creating a distributed sequence generator are as below :
// https://stackoverflow.com/questions/10338076/zookeeper-persistent-sequential-incrementing-by-two/36927266#36927266
// https://www.mail-archive.com/zookeeper-user@hadoop.apache.org/msg01967.html
public class DistributedSequenceGenerator {

    private static ZooKeeper zookeeper;

    //Latch is used to make the main thread wait until the zookeeper client connects with the ensemble.
    private CountDownLatch latch = new CountDownLatch(0);

    public static void main(String[] args){
        DistributedSequenceGenerator sequenceGenerator = new DistributedSequenceGenerator();
        try{
            zookeeper = sequenceGenerator.connect("localhost",2181);
            String out = zookeeper.create("/sequence","test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            System.out.println(out);
            sequenceGenerator.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public ZooKeeper connect(String host,int port) throws IOException, InterruptedException {
        zookeeper = new ZooKeeper(host, port, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                    if(watchedEvent.getState() == Event.KeeperState.SyncConnected){
                        latch.countDown();
                    }
            }
        });

        latch.await();
        return zookeeper;
    }

    public void close() throws InterruptedException{
        zookeeper.close();
    }
}
