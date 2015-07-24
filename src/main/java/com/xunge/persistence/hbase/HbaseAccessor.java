package com.xunge.persistence.hbase;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import com.xunge.persistence.hbase.api.ForEach;
import com.xunge.persistence.hbase.api.Row;
import com.xunge.persistence.hbase.api.Table;
import com.xunge.persistence.hbase.api.TypeConverter;

/**
 * HBase核心访问器(支持并发)
 *
 * @param <QUERY_OP_TYPE>
 * @param <ROW_ID_TYPE>
 * @author stereo
 * @version 2014.2.25
 * @Log1 试图给Hbase添加ACID支持(未完成 2014.9.1)
 * @Log2 读写诶分离, 写入和删除都是以队列的形式操作 (2014.2.28)
 * @Log3 延缓提交方式(一次写入，等到下次访问时提交/或者JVM停止时提交 2014.2.29)
 * @Log4 多线程处理暂时采用读写锁保证并发
 * @Log5 暂时HConnection代替HTablePool(2014.9.18)
 * @Log6 save(row原子)、delete、scan(复杂而低效)、fetch(高性能)、count(原子)
 */
public final class HbaseAccessor<QUERY_OP_TYPE extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> {

    private static final Log LOG = LogFactory.getLog(HbaseAccessor.class);

    private static final int MAX_QUEUE_SIZE = 1024;

    /**
     * put Queue
     */
    private Map<byte[], Queue<Put>> putsMap = new TreeMap<byte[], Queue<Put>>(
            Bytes.BYTES_COMPARATOR);

    /**
     * delete Queue
     */
    private Map<byte[], Queue<Delete>> deletesMap = new TreeMap<byte[], Queue<Delete>>(
            Bytes.BYTES_COMPARATOR);

    /**
     * fair true
     */
    private ReadWriteLock lock = new ReentrantReadWriteLock(true);

    private Configuration conf;

    // 0.94以后废弃
    private HTablePool pool;

    private HConnection connection;

    private Class<?> whereClauseType;

    private Class<ROW_ID_TYPE> idType;

    private static int poolMaxSize = 500;

    private static int poolCoreSize = 16;

    private TransactionManager<ROW_ID_TYPE> transactionManager;

    public HbaseAccessor(Class<ROW_ID_TYPE> idType) {
        this(idType, poolMaxSize);
    }

    @SuppressWarnings("unchecked")
    public HbaseAccessor(Class<ROW_ID_TYPE> idType, int poolMaxSize) {
        this((Class<QUERY_OP_TYPE>) (Class) QueryOps.class, idType, poolMaxSize);
    }

    public HbaseAccessor(Class<ROW_ID_TYPE> idType, Configuration conf) {
        this(idType, conf, poolMaxSize);
    }

    @SuppressWarnings("unchecked")
    public HbaseAccessor(Class<ROW_ID_TYPE> idType, Configuration conf,
                         int poolMaxSize) {
        this((Class<QUERY_OP_TYPE>) (Class) QueryOps.class, idType, conf,
                poolMaxSize);
    }

    public HbaseAccessor(Class<QUERY_OP_TYPE> whereClauseType,
                         Class<ROW_ID_TYPE> idType, int poolMaxSize) {
        this(whereClauseType, idType, HBaseConfiguration.create(), poolMaxSize);
    }

    public HbaseAccessor(Class<QUERY_OP_TYPE> whereClauseType,
                         Class<ROW_ID_TYPE> idType, Configuration conf, int poolMaxSize) {
        this.whereClauseType = whereClauseType;
        this.idType = idType;
        this.conf = conf;
        HbaseAccessor.poolMaxSize = poolMaxSize;
        this.pool = new HTablePool(conf, poolMaxSize);
        openConnection();
        setTransactionManager(new TransactionManager<ROW_ID_TYPE>(this));
        setupAutoFlushOnShutdown();
    }

    /**
     * 全表删除
     *
     * @param tableName
     */
    public void truncateTable(final String tableName) {
        truncateTable(Bytes.toBytes(tableName));
    }

    /**
     * 全表删除
     *
     * @param tableName
     */
    public void truncateTable(final byte[] tableName) {
        lock.writeLock().lock();
        scan(tableName).foreach(new ForEach<Row<ROW_ID_TYPE>>() {
            @Override
            public void process(Row<ROW_ID_TYPE> row) {
                delete(tableName).row(row.getId());
            }
        });
        flush(tableName);
        lock.writeLock().unlock();
    }

    /**
     * 获取CountRow递增Row的属性
     *
     * @param tableName
     * @return
     */
    public CountRow<QUERY_OP_TYPE, ROW_ID_TYPE> count(String tableName) {
        return count(Bytes.toBytes(tableName));
    }

    /**
     * 获取CountRow递增Row的属性
     *
     * @param tableName
     * @return
     */
    public CountRow<QUERY_OP_TYPE, ROW_ID_TYPE> count(byte[] tableName) {
        LOG.debug("count [" + tableName + "]");
        lock.writeLock().lock();
        try {
            return new CountRow<QUERY_OP_TYPE, ROW_ID_TYPE>(this, tableName);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 获取SaveRow存储Row
     *
     * @param tableName
     * @return
     */
    public SaveRow<QUERY_OP_TYPE, ROW_ID_TYPE> save(String tableName) {
        return save(Bytes.toBytes(tableName));
    }

    /**
     * 获取SaveRow存储Row
     *
     * @param tableName
     * @return
     */
    public SaveRow<QUERY_OP_TYPE, ROW_ID_TYPE> save(byte[] tableName) {
        LOG.debug("save [" + tableName + "]");
        lock.writeLock().lock();
        try {
            return new SaveRow<QUERY_OP_TYPE, ROW_ID_TYPE>(this, tableName);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 获取FetchRow查询Row
     *
     * @param tableName
     * @return
     */
    public FetchRow<ROW_ID_TYPE> fetch(String tableName) {
        return fetch(Bytes.toBytes(tableName));
    }

    /**
     * 获取FetchRow查询Row
     *
     * @param tableName
     * @return
     */
    public FetchRow<ROW_ID_TYPE> fetch(byte[] tableName) {
        flush();
        LOG.debug("fetch [" + tableName + "]");
        lock.readLock().lock();
        try {
            return new FetchRow<ROW_ID_TYPE>(this, tableName);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 定义数据表
     *
     * @param tableName
     * @return
     */
    public TableAdmin defineTable(String tableName) {
        return defineTable(Bytes.toBytes(tableName));
    }

    /**
     * 定义数据表
     *
     * @param tableName
     * @return
     */
    public TableAdmin defineTable(byte[] tableName) {
        flush();
        LOG.debug("defineTable [" + tableName + "]");
        return new TableAdmin(tableName, conf);
    }

    /**
     * 移除表
     *
     * @param tableName
     */
    public void removeTable(String tableName) {
        removeTable(Bytes.toBytes(tableName));
    }

    /**
     * 移除表
     *
     * @param tableName
     */
    public void removeTable(byte[] tableName) {
        flush();
        LOG.debug("removeTable [" + tableName + "]");
        try {
            HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
        } catch (TableNotFoundException e) {
            LOG.error("Table [" + tableName + "] does not exist.");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取Scanner进行扫描
     *
     * @param tableName
     * @return
     */
    public Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scan(String tableName) {
        return scan(tableName, null);
    }

    /**
     * 获取Scanner进行扫描
     *
     * @param tableName
     * @return
     */
    public Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scan(byte[] tableName) {
        return scan(tableName, null);
    }

    /**
     * 获取Scanner进行扫描
     *
     * @param tableName
     * @param startId
     * @return
     */
    public Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scan(String tableName,
                                                    ROW_ID_TYPE startId) {
        return scan(tableName, startId, null);
    }

    /**
     * 获取Scanner进行扫描
     *
     * @param tableName
     * @param startId
     * @return
     */
    public Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scan(byte[] tableName,
                                                    ROW_ID_TYPE startId) {
        return scan(tableName, startId, null);
    }

    /**
     * 获取Scanner进行扫描
     *
     * @param tableName
     * @param startId
     * @param endId
     * @return
     */
    public Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scan(String tableName,
                                                    ROW_ID_TYPE startId, ROW_ID_TYPE endId) {
        return scan(Bytes.toBytes(tableName), startId, endId);
    }

    /**
     * 获取Scanner进行扫描
     *
     * @param tableName
     * @param startId
     * @param endId
     * @return
     */
    public Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scan(byte[] tableName,
                                                    ROW_ID_TYPE startId, ROW_ID_TYPE endId) {
        flush();
        LOG.debug("scan [" + tableName + "] startId [" + startId + "] endId ["
                + endId + "]");
        lock.readLock().lock();
        try {
            HTable hTable = new HTable(conf, tableName);
            return new Scanner<QUERY_OP_TYPE, ROW_ID_TYPE>(this, hTable,
                    startId, endId);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取DeletedRow删除Row
     *
     * @param tableName
     * @return
     */
    public DeletedRow<QUERY_OP_TYPE, ROW_ID_TYPE> delete(String tableName) {
        return delete(Bytes.toBytes(tableName));
    }

    /**
     * 获取DeletedRow删除Row
     *
     * @param tableName
     * @return
     */
    public DeletedRow<QUERY_OP_TYPE, ROW_ID_TYPE> delete(byte[] tableName) {
        LOG.debug("delete [" + tableName + "]");
        lock.writeLock().lock();
        try {
            return new DeletedRow<QUERY_OP_TYPE, ROW_ID_TYPE>(this, tableName);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 注册转换类型
     *
     * @param typeConverter
     */
    public <U> void registerTypeConverter(TypeConverter<U> typeConverter) {
        Converter.registerType(typeConverter);
    }

    /**
     * 获取表操作并重新建立访问器
     *
     * @param tableName
     * @param conf
     * @param type
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <TYPE> Table<QueryOps<TYPE>, TYPE> table(
            final String tableName, HBaseConfiguration conf, Class<TYPE> type) {
        return new HbaseAccessor(type, conf, poolMaxSize).table(tableName);
    }

    /**
     * 获取表操作
     *
     * @param tableName
     * @return
     */
    public Table<QUERY_OP_TYPE, ROW_ID_TYPE> table(final String tableName) {
        return table(Bytes.toBytes(tableName));
    }

    /**
     * 获取表操作
     *
     * @param tableName
     * @return
     */
    public Table<QUERY_OP_TYPE, ROW_ID_TYPE> table(final byte[] tableName) {
        return new Table<QUERY_OP_TYPE, ROW_ID_TYPE>() {

            @Override
            public DeletedRow<QUERY_OP_TYPE, ROW_ID_TYPE> delete() {
                return HbaseAccessor.this.delete(tableName);
            }

            @Override
            public FetchRow<ROW_ID_TYPE> fetch() {
                return HbaseAccessor.this.fetch(tableName);
            }

            @Override
            public SaveRow<QUERY_OP_TYPE, ROW_ID_TYPE> save() {
                return HbaseAccessor.this.save(tableName);
            }

            @Override
            public Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scan() {
                return HbaseAccessor.this.scan(tableName);
            }

            @Override
            public Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scan(ROW_ID_TYPE startId) {
                return HbaseAccessor.this.scan(tableName, startId);
            }

            @Override
            public Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scan(
                    ROW_ID_TYPE startId, ROW_ID_TYPE endId) {
                return HbaseAccessor.this.scan(tableName, startId, endId);
            }

            @Override
            public CountRow<QUERY_OP_TYPE, ROW_ID_TYPE> count() {
                return HbaseAccessor.this.count(tableName);
            }
        };
    }

    /**
     * SaveRow ==> savePut
     *
     * @param tableName
     * @param put
     */
    protected void savePut(byte[] tableName, Put put) {
        lock.writeLock().lock();
        try {
            Queue<Put> puts = getPuts(tableName);
            if (puts.size() >= MAX_QUEUE_SIZE) {
                flushPuts(tableName, puts);
            }
            puts.add(put);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * DeletedRow ==> saveDelete
     *
     * @param tableName
     * @param delete
     */
    protected void saveDelete(byte[] tableName, Delete delete) {
        lock.writeLock().lock();
        try {
            Queue<Delete> deletes = getDeletes(tableName);
            if (deletes.size() >= MAX_QUEUE_SIZE) {
                flushDeletes(tableName, deletes);
            }
            deletes.add(delete);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Object ==> byte[]
     *
     * @param o
     * @return
     */
    protected byte[] toBytes(Object o) {
        if (o == null) {
            return null;
        }
        return Converter.toBytes(o);
    }

    /**
     * value ==> <U> U
     *
     * @param value
     * @param c
     * @return
     */
    protected <U> U fromBytes(byte[] value, Class<U> c) {
        if (value == null) {
            return null;
        }
        return Converter.fromBytes(value, c);
    }

    /**
     * 创建QUERY_OP_TYPE
     *
     * @param whereScanner
     * @param family
     * @param value
     * @return
     */
    @SuppressWarnings("unchecked")
    protected QueryOps<ROW_ID_TYPE> createWhereClause(
            Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> whereScanner,
            byte[] family, byte[] value) {
        try {
            Constructor<?> constructor = this.whereClauseType
                    .getConstructor(new Class[]{Where.class, byte[].class,
                            byte[].class});
            return (QueryOps<ROW_ID_TYPE>) constructor.newInstance(
                    whereScanner, family, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 转换结果集
     *
     * @param result
     * @return
     */
    protected Row<ROW_ID_TYPE> convert(Result result) {
        return new ResultRow<ROW_ID_TYPE>(this, result);
    }

    /**
     * 获取结果集
     *
     * @param tableName
     * @param get
     * @return
     */
    protected Result getResult(byte[] tableName, Get get) {
        HTableInterface table = pool.getTable(tableName);
        try {
            return table.get(get);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                pool.putTable(table);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 刷入Hbase
     */
    public void flush() {
        LOG.debug("flush");
        for (byte[] tableName : putsMap.keySet()) {
            flush(tableName);
        }
    }

    /**
     * 指定tableName刷入Hbase
     *
     * @param tableName
     */
    public void flush(String tableName) {
        flush(Bytes.toBytes(tableName));
    }

    /**
     * 指定tableName刷入Hbase
     *
     * @param tableName
     */
    public void flush(byte[] tableName) {
        lock.writeLock().lock();
        try {
            LOG.debug("flush [" + tableName + "]");
            Queue<Put> puts = getPuts(tableName);
            if (puts != null) {
                flushPuts(tableName, puts);
            }
            Queue<Delete> deletes = getDeletes(tableName);
            if (deletes != null) {
                flushDeletes(tableName, deletes);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Hbase刷入deletes
     *
     * @param tableName
     * @param deletes
     */
    protected void flushDeletes(byte[] tableName, Queue<Delete> deletes) {
        // 废弃pool
        // HTableInterface table = pool.getTable(tableName);
        HTableInterface table = null;
        HConnection connection = this.connection;
        lock.writeLock().lock();
        try {
            // connection = HConnectionManager.createConnection(conf);
            table = connection.getTable(tableName);
            table.delete(new ArrayList<Delete>(deletes));
            // table.flushCommits();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                // pool.putTable(table);
                if (table != null)
                    table.close();
                // if (connection != null)
                // connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            deletes.clear();
            lock.writeLock().unlock();
        }
    }

    /**
     * Hbase刷入puts
     *
     * @param tableName
     * @param puts
     */
    protected void flushPuts(byte[] tableName, Queue<Put> puts) {
        // 废弃pool
        // HTableInterface table = pool.getTable(tableName);
        HTableInterface table = null;
        HConnection connection = this.connection;
        lock.writeLock().lock();
        try {
            // connection = HConnectionManager.createConnection(conf);
            table = connection.getTable(tableName);
            table.put(new ArrayList<Put>(puts));
            // table.flushCommits();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                // pool.putTable(table);
                if (table != null)
                    table.close();
                // if (connection != null)
                // connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            puts.clear();
            lock.writeLock().unlock();
        }
    }

    /**
     * Hbase刷入increment
     *
     * @param tableName
     * @param increment
     */
    protected void flushCount(byte[] tableName, Increment increment) {
        HTableInterface table = null;
        HConnection connection = this.connection;
        lock.writeLock().lock();
        try {
            table = connection.getTable(tableName);
            table.setAutoFlush(false);
            table.increment(increment);
            table.flushCommits();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (table != null)
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            lock.writeLock().unlock();
        }
    }

    /**
     * KeyRow Type
     *
     * @return
     */
    protected Class<ROW_ID_TYPE> getIdType() {
        return (Class<ROW_ID_TYPE>) idType;
    }

    /**
     * @param tableName
     * @return
     */
    private Queue<Put> getPuts(byte[] tableName) {
        Queue<Put> queue = putsMap.get(tableName);
        if (queue == null) {
            queue = new ConcurrentLinkedQueue<Put>();
            putsMap.put(tableName, queue);
        }
        return queue;
    }

    /**
     * @param tableName
     * @return
     */
    private Queue<Delete> getDeletes(byte[] tableName) {
        Queue<Delete> queue = deletesMap.get(tableName);
        if (queue == null) {
            queue = new ConcurrentLinkedQueue<Delete>();
            deletesMap.put(tableName, queue);
        }
        return queue;
    }

    /**
     * 获取事务(未实现)
     *
     * @return
     */
    public TransactionManager<ROW_ID_TYPE> getTransactionManager() {
        return transactionManager;
    }

    /**
     * (未实现)
     *
     * @param transactionManager
     */
    protected void setTransactionManager(
            TransactionManager<ROW_ID_TYPE> transactionManager) {
        this.transactionManager = transactionManager;
    }

    /**
     * 打开连接
     * <p/>
     * 采用有界队列处理,资源好紧会采用CallerRunsPolicy(自悬,直到可以加入)来减缓任务
     */
    public void openConnection() {
        try {
            // 创建连接池
            connection = HConnectionManager
                    .createConnection(conf, new ThreadPoolExecutor(
                            poolCoreSize, poolMaxSize, 0L,
                            TimeUnit.MILLISECONDS,
                            new ArrayBlockingQueue<Runnable>(MAX_QUEUE_SIZE),
                            // new LinkedBlockingQueue<Runnable>(),
                            new Daemon.DaemonFactory(), new CallerRunsPolicy()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭连接
     */
    public void closeConnection() {
        if (!connection.isClosed())
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    private void setupAutoFlushOnShutdown() {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                flush();
                closeConnection();
            }
        }));
    }

    @Override
    protected void finalize() throws Throwable {
        flush();
        closeConnection();
    }

    public static void main(String[] args) {
        HbaseAccessor accessor = new HbaseAccessor(String.class);
        accessor.openConnection();
        accessor.closeConnection();

        for (; ; ) ;
    }
}