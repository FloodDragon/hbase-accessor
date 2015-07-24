package com.xunge.persistence.hbase;

import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.xunge.persistence.hbase.api.Transaction;

class TransactionState implements Transaction {

    private boolean rollbackOnly;
    private long startTimestamp;
    private long commitTimestamp;
    private Map<byte[], Queue<Put>> putsMap = new TreeMap<byte[], Queue<Put>>(
            Bytes.BYTES_COMPARATOR);
    private Map<byte[], Queue<Delete>> deletesMap = new TreeMap<byte[], Queue<Delete>>(
            Bytes.BYTES_COMPARATOR);

    TransactionState(long startTimestamp) {
        this.startTimestamp = startTimestamp;
        this.commitTimestamp = 0;
    }

    protected void savePut(byte[] tableName, Put put) {
        Queue<Put> puts = getPuts(tableName);
        puts.add(put);
    }

    protected void saveDelete(byte[] tableName, Delete delete) {
        Queue<Delete> deletes = getDeletes(tableName);
        deletes.add(delete);
    }

    private Queue<Put> getPuts(byte[] tableName) {
        Queue<Put> queue = putsMap.get(tableName);
        if (queue == null) {
            queue = new ConcurrentLinkedQueue<Put>();
            putsMap.put(tableName, queue);
        }
        return queue;
    }

    private Queue<Delete> getDeletes(byte[] tableName) {
        Queue<Delete> queue = deletesMap.get(tableName);
        if (queue == null) {
            queue = new ConcurrentLinkedQueue<Delete>();
            deletesMap.put(tableName, queue);
        }
        return queue;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    public void setCommitTimestamp(long commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    public String toString() {
        return "Transaction-" + Long.toHexString(startTimestamp);
    }

    @Override
    public void setRollbackOnly() {
        rollbackOnly = true;
    }

    public boolean isRollbackOnly() {
        return rollbackOnly;
    }

    public Map<byte[], Queue<Put>> getPutsMap() {
        return putsMap;
    }

    public Map<byte[], Queue<Delete>> getDeletesMap() {
        return deletesMap;
    }
}