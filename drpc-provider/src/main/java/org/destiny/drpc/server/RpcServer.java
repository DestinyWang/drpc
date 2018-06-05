package org.destiny.drpc.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.commons.collections4.MapUtils;
import org.destiny.drpc.annotations.RpcService;
import org.destiny.drpc.handler.RpcDecoder;
import org.destiny.drpc.handler.RpcEncoder;
import org.destiny.drpc.handler.RpcHandler;
import org.destiny.drpc.pojo.RpcRequest;
import org.destiny.drpc.pojo.RpcResponse;
import org.destiny.drpc.registry.ServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 王康
 * destinywk@163.com
 * ------------------------------------------------------------------
 * <p>
 *     @see org.springframework.context.ApplicationContextAware
 *     能够获取 Spring 的上下文 applicationContext，从而快速进行调用
 *
 *     @see org.springframework.beans.factory.InitializingBean
 *     为 Bean 提供了初始化方法的方式，只包括 afterProertiesSet 方法，
 *     在初始化 Bean 的时候会执行该方法
 * </p>
 * ------------------------------------------------------------------
 * Corpright 2018 Netease, Inc. All rights reserved.
 * NETEASE PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 * @version JDK 1.8.0_101
 * @since 2017/8/22 16:38
 */
public class RpcServer implements ApplicationContextAware, InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(RpcServer.class);

    /**
     * 服务端地址
     */
    private String serverAddress;

    /**
     * 注册中心
     */
    private ServiceRegistry serviceRegistry;

    /**
     * 存放接口名与服务对象之间的映射关系
     */
    private Map<String, Object> handlerMap = new ConcurrentHashMap<>();

    public RpcServer(String serverAddress) {
        this.serverAddress = serverAddress;
    }

    public RpcServer(String serverAddress, ServiceRegistry serviceRegistry) {
        this.serverAddress = serverAddress;
        this.serviceRegistry = serviceRegistry;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        // 获取所有带有注解 RpcService 的 Spring Bean
        Map<String, Object> serviceBeanMap = applicationContext.getBeansWithAnnotation(RpcService.class);
        if (MapUtils.isNotEmpty(serviceBeanMap)) {
            for (Object service : serviceBeanMap.values()) {
                // 获取 @RpcService 注解中 value 属性的值，即实现接口的全路径名
                String interfaceName = service.getClass().getAnnotation(RpcService.class).value().getName();
                handlerMap.put(interfaceName, service);
            }
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class).childHandler(new ChannelInitializer<SocketChannel>() {
                @Override
                protected void initChannel(SocketChannel socketChannel) throws Exception {
                    socketChannel.pipeline()
                            .addLast(new RpcDecoder(RpcRequest.class))
                            .addLast(new RpcEncoder(RpcResponse.class))
                            .addLast(new RpcHandler(handlerMap));
                }

            }).option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);
            String[] array = serverAddress.split(":");
            String host = array[0];
            int port = Integer.valueOf(array[1]);

            ChannelFuture future = bootstrap.bind(host, port).sync();
            logger.info("server started on port {}", port);

            if (serviceRegistry != null) {
                // 注册服务地址
                for (String serverName : handlerMap.keySet()) {
                    serviceRegistry.register("/" + serverName, serverAddress);
                }
            }

            future.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }


}
