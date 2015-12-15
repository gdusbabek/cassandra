package org.apache.cassandra.service;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.ClientMetrics;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.RequestThreadPoolExecutor;
import org.apache.cassandra.transport.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class DomainTransportService
{
    private static final Logger logger = LoggerFactory.getLogger(DomainTransportService.class);
    
    private Collection<Server> servers = Collections.emptyList();
    
    private boolean initialized = false;
    private EventLoopGroup workerGroup;
    private EventExecutor eventExecutorGroup;
    
    @VisibleForTesting
    synchronized void initialize() 
    {
        if (initialized)
            return;

        // prepare netty resources
        eventExecutorGroup = new RequestThreadPoolExecutor();

        if (useEpoll())
        {
            workerGroup = new EpollEventLoopGroup();
            logger.info("Netty using native Epoll event loop");
        }
        else
        {
            workerGroup = new NioEventLoopGroup();
            logger.info("Netty using Java NIO event loop");
        }

        DomainSocketAddress domainSocketAddress = DatabaseDescriptor.getDomainAddress();
        
        org.apache.cassandra.transport.Server.Builder builder = new org.apache.cassandra.transport.Server.Builder()
                .withEventExecutor(eventExecutorGroup)
                .withEventLoopGroup(workerGroup)
                .withDomainSocket(domainSocketAddress)
                .withChannel(useEpoll() ? EpollServerDomainSocketChannel.class : NioServerSocketChannel.class)
                .withBootstrapChildOption(ChannelOption.ALLOCATOR, CBUtil.allocator);
        
        if (DatabaseDescriptor.getClientEncryptionOptions().enabled) {
            builder = builder.withSSL(true);
        }
        
        servers = Collections.singleton(builder.build());
        
        // register metrics
        ClientMetrics.instance.addCounter("connectedDomainClients", () ->
        {
            int ret = 0;
            for (Server server : servers)
                ret += server.getConnectedClients();
            return ret;
        });

        initialized = true;
    }
    
    /**
     * Starts native transport servers.
     */
    public void start()
    {
        initialize();
        servers.forEach(Server::start);
    }
    
    /**
     * Stops currently running native transport servers.
     */
    public void stop()
    {
        servers.forEach(Server::stop);
    }

    /**
     * Ultimately stops servers and closes all resources.
     */
    public void destroy()
    {
        stop();
        servers = Collections.emptyList();

        // shutdown executors used by netty for native transport server
        Future<?> wgStop = workerGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);

        try
        {
            wgStop.await(5000);
        }
        catch (InterruptedException e1)
        {
            Thread.currentThread().interrupt();
        }

        // shutdownGracefully not implemented yet in RequestThreadPoolExecutor
        eventExecutorGroup.shutdown();
    }

    /**
     * @return intend to use epoll bassed event looping
     */
    public static boolean useEpoll()
    {
        final boolean enableEpoll = Boolean.valueOf(System.getProperty("cassandra.native.epoll.enabled", "true"));
        return enableEpoll && Epoll.isAvailable();
    }

    /**
     * @return true in case native transport server is running
     */
    public boolean isRunning()
    {
        for (Server server : servers)
            if (server.isRunning()) return true;
        return false;
    }

    @VisibleForTesting
    EventLoopGroup getWorkerGroup()
    {
        return workerGroup;
    }

    @VisibleForTesting
    EventExecutor getEventExecutor()
    {
        return eventExecutorGroup;
    }

    @VisibleForTesting
    Collection<Server> getServers()
    {
        return servers;
    }
}
