/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ctrip.hermes.remoting;

import java.net.InetSocketAddress;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.remoting.ChannelEventListener;
import com.alibaba.rocketmq.remoting.InvokeCallback;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.RemotingServer;
import com.alibaba.rocketmq.remoting.common.Pair;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;


/**
 * Remoting服务端实现
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-13
 */
public class NettyRemotingServer {
    private static final Logger log = LoggerFactory.getLogger(RemotingHelper.RemotingLogName);
    private final ServerBootstrap serverBootstrap;
    private final EventLoopGroup eventLoopGroupWorker;
    private final EventLoopGroup eventLoopGroupBoss;
    private final NettyServerConfig nettyServerConfig;
    // 处理Callback应答器
    private final ExecutorService publicExecutor;
    private final ChannelEventListener channelEventListener;
    // 定时器
    private final Timer timer = new Timer("ServerHouseKeepingService", true);
    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    private RPCHook rpcHook;

    // 本地server绑定的端口
    private int port = 0;


    public NettyRemotingServer(final NettyServerConfig nettyServerConfig) {
        this(nettyServerConfig, null);
    }


    public NettyRemotingServer(final NettyServerConfig nettyServerConfig,
            final ChannelEventListener channelEventListener) {
//        super(nettyServerConfig.getServerOnewaySemaphoreValue(), nettyServerConfig
//            .getServerAsyncSemaphoreValue());
        this.serverBootstrap = new ServerBootstrap();
        this.nettyServerConfig = nettyServerConfig;
        this.channelEventListener = channelEventListener;

        int publicThreadNums = nettyServerConfig.getServerCallbackExecutorThreads();
        if (publicThreadNums <= 0) {
            publicThreadNums = 4;
        }

        this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);


            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "NettyServerPublicExecutor_" + this.threadIndex.incrementAndGet());
            }
        });

        this.eventLoopGroupBoss = new NioEventLoopGroup(1, new ThreadFactory() {
            private AtomicInteger threadIndex = new AtomicInteger(0);


            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r,
                    String.format("NettyBossSelector_%d", this.threadIndex.incrementAndGet()));
            }
        });

        this.eventLoopGroupWorker =
                new NioEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactory() {
                    private AtomicInteger threadIndex = new AtomicInteger(0);
                    private int threadTotal = nettyServerConfig.getServerSelectorThreads();


                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, String.format("NettyServerSelector_%d_%d", threadTotal,
                            this.threadIndex.incrementAndGet()));
                    }
                });
    }


    @Override
    public void start() {
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(//
            nettyServerConfig.getServerWorkerThreads(), //
            new ThreadFactory() {

                private AtomicInteger threadIndex = new AtomicInteger(0);


                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "NettyServerWorkerThread_" + this.threadIndex.incrementAndGet());
                }
            });

        ServerBootstrap childHandler = //
                this.serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupWorker)
                    .channel(NioServerSocketChannel.class)
                    //
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    //
                    .option(ChannelOption.SO_REUSEADDR, true)
                    //
                    .option(ChannelOption.SO_KEEPALIVE, false)
                    //
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    //
                    .option(ChannelOption.SO_SNDBUF, nettyServerConfig.getServerSocketSndBufSize())
                    //
                    .option(ChannelOption.SO_RCVBUF, nettyServerConfig.getServerSocketRcvBufSize())
                    //
                    .localAddress(new InetSocketAddress(this.nettyServerConfig.getListenPort()))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
//                            ch.pipeline().addLast(
//                                //
//                                defaultEventExecutorGroup, //
////                                new NettyEncoder(), //
////                                new NettyDecoder(), //
////                                new IdleStateHandler(0, 0, nettyServerConfig
////                                    .getServerChannelMaxIdleTimeSeconds()),//
////                                new NettyConnetManageHandler(), //
////                                new NettyServerHandler());
                        }
                    });

        if (nettyServerConfig.isServerPooledByteBufAllocatorEnable()) {
            // 这个选项有可能会占用大量堆外内存，暂时不使用。
            childHandler.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        }

        try {
            ChannelFuture sync = this.serverBootstrap.bind().sync();
            InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
            this.port = addr.getPort();
        }
        catch (InterruptedException e1) {
            throw new RuntimeException("this.serverBootstrap.bind().sync() InterruptedException", e1);
        }

        if (this.channelEventListener != null) {
            this.nettyEventExecuter.start();
        }

        // 每隔1秒扫描下异步调用超时情况
        this.timer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                try {
                    NettyRemotingServer.this.scanResponseTable();
                }
                catch (Exception e) {
                    log.error("scanResponseTable exception", e);
                }
            }
        }, 1000 * 3, 1000);
    }


    @Override
    public void shutdown() {
        try {
            if (this.timer != null) {
                this.timer.cancel();
            }

            this.eventLoopGroupBoss.shutdownGracefully();

            this.eventLoopGroupWorker.shutdownGracefully();

            if (this.nettyEventExecuter != null) {
                this.nettyEventExecuter.shutdown();
            }

            if (this.defaultEventExecutorGroup != null) {
                this.defaultEventExecutorGroup.shutdownGracefully();
            }
        }
        catch (Exception e) {
            log.error("NettyRemotingServer shutdown exception, ", e);
        }

        if (this.publicExecutor != null) {
            try {
                this.publicExecutor.shutdown();
            }
            catch (Exception e) {
                log.error("NettyRemotingServer shutdown exception, ", e);
            }
        }
    }


}
