package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

public interface RecoveryContext {
    void receive(byte[] inputBuffer, int size, final long entryLocation, final CommitLogDescriptor desc) throws IOException;
    Map<UUID, ReplayPosition> getPositions();
}
