package org.apache.cassandra.service;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

public class CassandraFatDaemon {
    public static void main(String args[]) throws Throwable {
        try {
            DatabaseDescriptor.forceStaticInitialization();
        } catch (ExceptionInInitializerError e) {
            throw e.getCause();
        }
        Config.setClientMode(true);
        StorageService.instance.initClient();
    }
}
