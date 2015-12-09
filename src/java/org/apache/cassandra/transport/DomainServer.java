package org.apache.cassandra.transport;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import org.apache.cassandra.metrics.ClientMetrics;

import java.net.SocketAddress;
import java.util.concurrent.Callable;

public class DomainServer extends AbstractServer 
{
    private final DomainSocketAddress address;

    public DomainServer(DomainSocketAddress address, int transportPort)
    {
        super(transportPort);
        this.address = address;
    }

    @Override
    protected ServerBootstrap bootstrap(EventLoopGroup workerGroup)
    {
        return new ServerBootstrap()
                .group(workerGroup)
                .channel(getChannelType())
                .childOption(ChannelOption.ALLOCATOR, CBUtil.allocator);
    }
    

    @Override
    protected Class<? extends ServerChannel> getChannelType()
    {
        return hasEpoll ? EpollServerDomainSocketChannel.class : NioServerSocketChannel.class;
    }

    @Override
    protected void registerMetrics()
    {
        ClientMetrics.instance.addCounter("connectedDomainClients", new Callable<Integer>()
        {
            @Override
            public Integer call() throws Exception
            {
                return connectionTracker.getConnectedClients();
            }
        });
    }

    @Override
    protected SocketAddress socket()
    {
        return address;
    }
}
