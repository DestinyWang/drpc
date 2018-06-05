package org.destiny.drpc.proxy;

import com.alibaba.fastjson.JSON;
import org.destiny.drpc.client.RpcClient;
import org.destiny.drpc.discovery.ServiceDiscovery;
import org.destiny.drpc.pojo.RpcRequest;
import org.destiny.drpc.pojo.RpcResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.UUID;

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
public class RpcProxy {

    private String serverAddress;
    private ServiceDiscovery serviceDiscovery;
    private int port;

    private static final Logger logger = LoggerFactory.getLogger(RpcProxy.class);

    public RpcProxy(String serverAddress) {
        this.serverAddress = serverAddress;
    }

    public RpcProxy(ServiceDiscovery serviceDiscovery, int port) {
        this.serviceDiscovery = serviceDiscovery;
        this.port = port;
    }

    public <T> T create(Class<?> interfaceClass) {
        return (T) Proxy.newProxyInstance(interfaceClass.getClassLoader(), new Class<?>[]{interfaceClass}, new InvocationHandler(){
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                // 创建并初始化 RPC 请求
                RpcRequest request = new RpcRequest();
                request.setRequestId(UUID.randomUUID().toString());
                request.setClassName(method.getDeclaringClass().getName());
                request.setMethodName(method.getName());
                request.setParameterTypes(method.getParameterTypes());
                request.setParameters(args);

                logger.info("the RpcRequest is: {}", JSON.toJSONString(request));

                if (serviceDiscovery != null) {
                    // 发现服务
                    serverAddress = serviceDiscovery.discover(interfaceClass.getName());
                }

                logger.info("Get the IP from zookeeper: {}", serverAddress);

                String[] array = serverAddress.split(":");
                String host = array[0];

                // 初始化 RPC 客户端
                RpcClient client = new RpcClient(host, port);
                // 通过 RPC 客户端发送 RPC 请求并获取 RPC 响应
                RpcResponse response = client.send(request);

                if (response.isError()) {
                    throw response.getError();
                } else {
                    return response.getResult();
                }
            }
        });
    }
}
