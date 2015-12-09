/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.transport;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Callable;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.ClientMetrics;

public class Server extends AbstractServer
{
    public final InetSocketAddress socket;
    

    public Server(InetSocketAddress socket)
    {
        super(socket.getPort());
        this.socket = socket;
    }

    public Server(String hostname, int port)
    {
        this(new InetSocketAddress(hostname, port));
    }

    public Server(InetAddress host, int port)
    {
        this(new InetSocketAddress(host, port));
    }

    public Server(int port)
    {
        this(new InetSocketAddress(port));
    }

    @Override
    protected SocketAddress socket() 
    {
        return socket;
    }

    @Override
    protected Class<? extends ServerChannel> getChannelType() 
    {
        return hasEpoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class;
    }

    protected ServerBootstrap bootstrap(EventLoopGroup workerGroup)
    {
        return new ServerBootstrap()
            .group(workerGroup)
            .channel(getChannelType())
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childOption(ChannelOption.SO_LINGER, 0)
            .childOption(ChannelOption.SO_KEEPALIVE, DatabaseDescriptor.getRpcKeepAlive())
            .childOption(ChannelOption.ALLOCATOR, CBUtil.allocator)
            .childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024)
            .childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024);
    }

    protected void registerMetrics()
    {
        ClientMetrics.instance.addCounter("connectedNativeClients", new Callable<Integer>()
        {
            @Override
            public Integer call() throws Exception
            {
                return connectionTracker.getConnectedClients();
            }
        });
    }
}
