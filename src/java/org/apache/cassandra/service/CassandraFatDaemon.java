package org.apache.cassandra.service;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;

import java.net.InetAddress;

public class CassandraFatDaemon {
    private static final CassandraFatDaemon instance = new CassandraFatDaemon();
    private CassandraDaemon.Server nativeServer;
    
    
    public static void main(String args[]) throws Throwable {
        try {
            DatabaseDescriptor.forceStaticInitialization();
        } catch (ExceptionInInitializerError e) {
            throw e.getCause();
        }
        //Config.setClientMode(true);
        
        // load schema from disk
        Schema.instance.loadFromDisk();
        Keyspace.setInitialized();
        SystemKeyspace.finishStartup();
        
        StorageService.instance.initClient();
        
        InetAddress nativeAddr = DatabaseDescriptor.getRpcAddress();
        int nativePort = DatabaseDescriptor.getNativeTransportPort();
        instance.nativeServer = new org.apache.cassandra.transport.Server(nativeAddr, nativePort);
        instance.nativeServer.start();
    }
}
