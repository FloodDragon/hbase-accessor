package com.xunge.persistence.hbase;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;

import com.xunge.persistence.hbase.api.Transaction;
import com.xunge.persistence.hbase.exc.RollbackException;
import com.xunge.persistence.hbase.exc.TransactionException;

/**
 * Hbase 事务控制(未完成)
 *
 * @param <ROW_ID_TYPE>
 * @author stereo
 */
public class TransactionManager<ROW_ID_TYPE> {

    private static final Log LOG = LogFactory.getLog(TransactionManager.class);
    private HbaseAccessor<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> hBase;
    private ThreadLocal<Transaction> threadLocal = new ThreadLocal<Transaction>();

    public TransactionManager(
            HbaseAccessor<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> hbaseAccessor) {
        this.hBase = hbaseAccessor;
    }

    public Transaction getTransaction() {
        return threadLocal.get();
    }

    /**
     * 开启事务
     *
     * @throws com.xunge.persistence.hbase.exc.TransactionException
     */
    public void begin() throws TransactionException {
        Transaction transaction = new TransactionState(
                System.currentTimeMillis());
        threadLocal.set(transaction);
    }

    /**
     * 提交
     *
     * @throws com.xunge.persistence.hbase.exc.RollbackException
     * @throws com.xunge.persistence.hbase.exc.TransactionException
     */
    public void commit() throws RollbackException, TransactionException {
        Transaction transaction = threadLocal.get();
        if (LOG.isTraceEnabled()) {
            LOG.trace("commit " + transaction);
        }
        if (!(transaction instanceof TransactionState)) {
            throw new IllegalArgumentException(
                    "transaction should be an instance of "
                            + TransactionState.class);
        }

        TransactionState transactionState = (TransactionState) transaction;
        Map<byte[], Queue<Delete>> dels = transactionState.getDeletesMap();
        Map<byte[], Queue<Put>> puts = transactionState.getPutsMap();
        Set<byte[]> tableNames = new HashSet<byte[]>();
        try {
            // for (Iterator<Entry<byte[], Queue<Delete>>> it = dels.entrySet()
            // .iterator(); it.hasNext();) {
            // Entry<byte[], Queue<Delete>> entry = it.next();
            // tableNames.add(entry.getKey());
            // hBase.flushDeletes(entry.getKey(), entry.getValue());
            // }
        } catch (Exception e) {
        }

        if (transactionState.isRollbackOnly()) {
            rollback(transactionState);
            throw new RollbackException();
        }
        transactionState.setCommitTimestamp(System.currentTimeMillis());
    }

    /**
     * 回滚
     *
     * @param transaction
     */
    public void rollback(Transaction transaction) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("abort " + transaction);
        }

        if (!(transaction instanceof TransactionState)) {
            throw new IllegalArgumentException(
                    "transaction should be an instance of "
                            + TransactionState.class);
        }
        // TransactionState transactionState = (TransactionState) transaction;
        //
        // try {
        // tsoclient.abort(transactionState.getStartTimestamp());
        // } catch (Exception e) {
        // LOG.warn("Couldn't notify TSO about the abort", e);
        // }
        //
        // if (LOG.isTraceEnabled()) {
        // LOG.trace("doneAbort " + transactionState.getStartTimestamp());
        // }
        //
        // // Make sure its commit timestamp is 0, so the cleanup does the right
        // // job
        // transactionState.setCommitTimestamp(0);
        // cleanup(transactionState);
    }

    /**
     * 清理
     *
     * @param transactionState
     */
    private void cleanup(final TransactionState transactionState) {
        // Map<byte[], List<Delete>> deleteBatches = new HashMap<byte[],
        // List<Delete>>();
        // for (final RowKeyFamily rowkey : transactionState.getRows()) {
        // List<Delete> batch = deleteBatches.get(rowkey.getTable());
        // if (batch == null) {
        // batch = new ArrayList<Delete>();
        // deleteBatches.put(rowkey.getTable(), batch);
        // }
        // Delete delete = new Delete(rowkey.getRow());
        // for (Entry<byte[], List<KeyValue>> entry : rowkey.getFamilies()
        // .entrySet()) {
        // for (KeyValue kv : entry.getValue()) {
        // delete.deleteColumn(entry.getKey(), kv.getQualifier(),
        // transactionState.getStartTimestamp());
        // }
        // }
        // batch.add(delete);
        // }
        //
        // boolean cleanupFailed = false;
        // List<HTable> tablesToFlush = new ArrayList<HTable>();
        // for (final Entry<byte[], List<Delete>> entry :
        // deleteBatches.entrySet()) {
        // try {
        // HTable table = tableCache.get(entry.getKey());
        // if (table == null) {
        // table = new HTable(conf, entry.getKey());
        // table.setAutoFlush(false, true);
        // tableCache.put(entry.getKey(), table);
        // }
        // table.delete(entry.getValue());
        // tablesToFlush.add(table);
        // } catch (IOException ioe) {
        // cleanupFailed = true;
        // }
        // }
        // for (HTable table : tablesToFlush) {
        // try {
        // table.flushCommits();
        // } catch (IOException e) {
        // cleanupFailed = true;
        // }
        // }
        //
        // if (cleanupFailed) {
        // LOG.warn("Cleanup failed, some values not deleted");
        // // we can't notify the TSO of completion
        // return;
        // }
        // AbortCompleteCallback cb = new SyncAbortCompleteCallback();
        // try {
        // tsoclient.completeAbort(transactionState.getStartTimestamp(), cb);
        // } catch (IOException ioe) {
        // LOG.warn("Coudldn't notify the TSO of rollback completion", ioe);
        // }
    }
}
