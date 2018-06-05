package org.destiny.drpc.utils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

/**
 * @author 王康
 * hzwangkang1@corp.netease.com
 * ------------------------------------------------------------------
 * <p></p>
 * ------------------------------------------------------------------
 * Corpright 2018 Netease, Inc. All rights reserved.
 * NETEASE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * @version JDK 1.8.0_101
 * @since 2017/8/22 16:38
 */
public class NetUtil {

    public static InetAddress getLocalHostLANAddress() throws SocketException, UnknownHostException {
        try {
            InetAddress candidateAddress = null;
            // 遍历所有网络接口
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaces.nextElement();
                // 在所有的接口下再遍历 IP
                Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    InetAddress inetAddress = inetAddresses.nextElement();
                    if (!inetAddress.isLoopbackAddress()) {
                        // 排除 loopback 类型地址
                        if (inetAddress.isSiteLocalAddress()) {
                            // 如果是 site-local 地址，直接返回
                            return inetAddress;
                        } else if (candidateAddress == null) {
                            // 如果是 site-local 地址未被发现，先记录候选地址
                            candidateAddress = inetAddress;
                        }
                    }
                }
            }
            if (candidateAddress != null) {
                return candidateAddress;
            }
        }catch (Exception e) {
            System.err.println("获取本机 IP 失败");
            e.printStackTrace();
        }
        return InetAddress.getLocalHost();
    }

    public static void main(String[] args) throws SocketException, UnknownHostException {
        System.out.println(NetUtil.getLocalHostLANAddress().getHostAddress());
    }
}
