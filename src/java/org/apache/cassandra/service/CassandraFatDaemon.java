package org.apache.cassandra.service;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.RecoveryContext;
import org.apache.cassandra.db.commitlog.ReplayPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class CassandraFatDaemon {
    private static final Logger log = LoggerFactory.getLogger(CassandraFatDaemon.class);
    
    private CassandraDaemon.Server nativeServer;
    
    private static void writeLastReplayPosition(ReplayPosition pos) throws Exception {
        DataOutputStream out = new DataOutputStream(new FileOutputStream("/tmp/last_replay"));
        out.writeLong(pos.segment);
        out.writeInt(pos.position);
        out.close();
    }
    
    private static ReplayPosition readLastReplayPosition() throws Exception {
        DataInputStream in = new DataInputStream(new FileInputStream("/tmp/last_replay"));
        ReplayPosition pos = new ReplayPosition(in.readLong(), in.readInt());
        in.close();
        return pos;
    }
    
    public static void main(String args[]) throws Throwable {
        final CassandraFatDaemon instance = new CassandraFatDaemon();
        instance.startCassandraInternals();
        
        Random random = new Random(System.nanoTime());
        
        // fun with alternate commit logs.
        final UUID group = UUID.randomUUID();
        final String commitLogPath = "/tmp/cl_357"; // "/tmp/cl_" + random.nextInt(1000)
        CommitLog cl = CommitLog.construct(commitLogPath);
        
        try {
            final ReplayPosition lastPosition = readLastReplayPosition();
            log.info("RECOVERING!!!");
            cl.recover(new RecoveryContext() {
                @Override
                public void receive(byte[] inputBuffer, int size, long entryLocation, CommitLogDescriptor desc) throws IOException {
                    System.out.println(String.format("recovering %d (%d) bytes found at %d in %s", size, inputBuffer.length, entryLocation, desc.fileName()));
                }

                @Override
                public Map<UUID, ReplayPosition> getPositions() {
                    return new HashMap<UUID, ReplayPosition>() {{
                        put(group, lastPosition);
                    }};
                }
            });
        } catch (IOException ex) {
            // nothing to recover.
            log.info("NOTHING TO RECOVER");
        }
        
        int limit = 100000000;
        StringBuilder sb = new StringBuilder("AAABBBCCCDDDEEE");
        byte[] buf = sb.toString().getBytes();
        ReplayPosition lastPos = null;
        ReplayPosition lastDiscard = null;
        
        for (int i = 0; i < limit; i++) {
            lastPos = cl.add(group, buf);
            if (Math.abs(random.nextInt()) % 3000000 == 1) {
                lastDiscard = lastPos;
                cl.discardCompletedSegments(group, lastDiscard);
                CassandraFatDaemon.writeLastReplayPosition(lastDiscard);
            }
        }

        System.out.println(String.format("content size:%d on-disk size:%d", cl.getActiveContentSize(), cl.getActiveOnDiskSize()));
        
        System.exit(0);
    }
    
    private void startCassandraInternals() throws Throwable {
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
        this.nativeServer = new org.apache.cassandra.transport.Server(nativeAddr, nativePort);
        this.nativeServer.start();
    }
}
