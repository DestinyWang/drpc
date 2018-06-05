package org.destiny.drpc.discovery;

import com.alibaba.fastjson.JSON;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.destiny.drpc.utils.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author 王康
 * destinywk@163.com
 * ------------------------------------------------------------------
 * <p></p>
 * ------------------------------------------------------------------
 * Corpright 2018 Netease, Inc. All rights reserved.
 * NETEASE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * @version JDK 1.8.0_101
 * @since 2017/8/22 16:38
 */
public class ServiceDiscovery {

    private static final Logger logger = LoggerFactory.getLogger(ServiceDiscovery.class);

    private CountDownLatch latch = new CountDownLatch(1);

//    private volatile List<String> dataList = new ArrayList<>();

    private String registryAddress;

    private Map<String, List<String>> serverAddressMap = new ConcurrentHashMap<>();

    public ServiceDiscovery(String registryAddress) {
        this.registryAddress = registryAddress;

        ZooKeeper zooKeeper = connectServer();
        if (zooKeeper != null) {
            watchNode(zooKeeper);
        }
    }

    public String discover(String serverName) {
        String data = null;
        List<String> dataList = serverAddressMap.get(serverName);
        int size = dataList.size();
        serverAddressMap.get(serverName);
        if (size > 0) {
            if (size == 1) {
                data = dataList.get(0);
                logger.debug("using only data: {}", data);
            } else {
                // 如果地址有多个, 那么随机挑选一个
                data = dataList.get(ThreadLocalRandom.current().nextInt(size));
                logger.debug("using random data: {}", data);
            }
        }
        return data;
    }

    private ZooKeeper connectServer() {
        ZooKeeper zk = null;
        try {
            zk = new ZooKeeper(registryAddress, Constant.ZK_SESSION_TIMEOUT, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        latch.countDown();
                    }
                }
            });
            latch.await();
            logger.info("zookeeper is connected to server");
        } catch (IOException | InterruptedException e) {
            logger.error("", e);
        }
        return zk;
    }

    private void watchNode(final ZooKeeper zk) {
        try {
            List<String> nodeList = zk.getChildren(Constant.ZK_REGISTRY_PATH, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeChildrenChanged) {
                        watchNode(zk);
                    }
                }
            });
//            List<String> dataList = new ArrayList<>();
            List<String> children = zk.getChildren(Constant.ZK_DATA_PATH, true);
            if (children != null && children.size() > 0) {
                for (String child : children) {
                    logger.debug("current path: {}", Constant.ZK_DATA_PATH + "/" + child);
                    List<String> addressList = zk.getChildren(Constant.ZK_DATA_PATH + "/" + child, true);
                    serverAddressMap.put(child, addressList);
                }
            } else {
                logger.error("children invalid");
            }
            logger.info("serverAddressMap: {}", JSON.toJSONString(serverAddressMap));
//            for (String node : nodeList) {
//                byte[] bytes = zk.getData(Constant.ZK_REGISTRY_PATH + "/" + node, false, null);
//                dataList.add(new String(bytes));
//            }
//            logger.debug("node data: {}", dataList);
//            this.dataList = dataList;
        } catch (KeeperException | InterruptedException e) {
            logger.error("", e);
        }
    }
}
