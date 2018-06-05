package org.destiny.drpc.registry;

import org.apache.zookeeper.*;
import org.destiny.drpc.utils.Constant;
import org.destiny.drpc.utils.NetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;

/**
 * @author 王康
 * hzwangkang1@corp.netease.com
 * ------------------------------------------------------------------
 * <p>
 *     服务注册
 * </p>
 * ------------------------------------------------------------------
 * Corpright 2018 Netease, Inc. All rights reserved.
 * NETEASE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * @version JDK 1.8.0_101
 * @since 2017/8/22 16:38
 */
public class ServiceRegistry {

    private static final Logger logger = LoggerFactory.getLogger(ServiceRegistry.class);

    private static final CountDownLatch COUNT_DOWN_LATCH = new CountDownLatch(1);

    private String registryAddress;

    public ServiceRegistry(String registryAddress) {
        this.registryAddress = registryAddress;
    }

    public void register(String serverName, String data) {
        if (data != null) {
            ZooKeeper zooKeeper = connectServer();
            if (zooKeeper != null) {
                createNode(zooKeeper, serverName, data);
            }
        }
    }

    private ZooKeeper connectServer() {
        ZooKeeper zooKeeper = null;
        try {
            zooKeeper = new ZooKeeper(registryAddress, Constant.ZK_SESSION_TIMEOUT, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                        COUNT_DOWN_LATCH.countDown();
                    }
                }
            });
            COUNT_DOWN_LATCH.await();
        } catch (IOException | InterruptedException e) {
            logger.error(e.toString());
        }
        return zooKeeper;
    }

    private void createNode(ZooKeeper zooKeeper, String serverName, String data) {
        try {
            byte[] bytes = data.getBytes(Charset.defaultCharset());
            String path = null;
            // 根路径
            if (zooKeeper.exists(Constant.ZK_REGISTRY_PATH, true) == null) {
                path = zooKeeper.create(Constant.ZK_REGISTRY_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            // 二级路径
            if (zooKeeper.exists(Constant.ZK_DATA_PATH, true) == null) {
                path = zooKeeper.create(Constant.ZK_DATA_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            //
            if (zooKeeper.exists(Constant.ZK_DATA_PATH + serverName, true) == null) {
                path = zooKeeper.create(Constant.ZK_DATA_PATH + serverName, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
//            else {
//                 如果已创建, 则重新设置内容
//                zooKeeper.setData(Constant.ZK_DATA_PATH + serverName, bytes, 0);
//            }
            // 获取本机 IP
            String hostAddress = NetUtil.getLocalHostLANAddress().getHostAddress();
            if (zooKeeper.exists(Constant.ZK_DATA_PATH + serverName + "/" + hostAddress, true) == null) {
                path = zooKeeper.create(Constant.ZK_DATA_PATH + serverName + "/" + hostAddress, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            }
            logger.info("create zookeeper node ({} => {})", path, data);
        } catch (InterruptedException | KeeperException e) {
            logger.error(e.toString());
        } catch (SocketException | UnknownHostException e) {
            logger.error("Error to get local host IP: {}", e.getMessage());
        }
    }
}
