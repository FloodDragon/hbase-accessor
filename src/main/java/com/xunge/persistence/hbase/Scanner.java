package com.xunge.persistence.hbase;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;

import com.xunge.persistence.hbase.api.ForEach;
import com.xunge.persistence.hbase.api.Row;

/**
 * 表扫描器(复杂查询)
 * <p/>
 * Scanner <==> HbaseAccessor Scanner <==> Select、Select ==> HbaseAccessor
 * Scanner <==> Where、Where ==> HbaseAccessor、Where <==> QueryOps
 *
 * @param <QUERY_OP_TYPE>
 * @param <ROW_ID_TYPE>
 * @author stereo
 */
public class Scanner<QUERY_OP_TYPE extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE>
        implements Iterable<Row<ROW_ID_TYPE>> {

    private HTable hTable;
    private Scan scan;
    private HbaseAccessor<QUERY_OP_TYPE, ROW_ID_TYPE> hBase;

    Scanner(HbaseAccessor<QUERY_OP_TYPE, ROW_ID_TYPE> hBase, HTable hTable,
            ROW_ID_TYPE startId, ROW_ID_TYPE endId) {
        this.hTable = hTable;
        this.hBase = hBase;
        if (startId != null && endId != null) {
            this.scan = new Scan(hBase.toBytes(startId), hBase.toBytes(endId));
        } else if (startId != null) {
            this.scan = new Scan(hBase.toBytes(startId));
        } else {
            this.scan = new Scan();
        }
    }

    /**
     * 数据迭代才开始查询Hbase
     */
    @Override
    public Iterator<Row<ROW_ID_TYPE>> iterator() {
        try {
            ResultScanner scanner = hTable.getScanner(scan);
            final Iterator<Result> iterator = scanner.iterator();
            return new Iterator<Row<ROW_ID_TYPE>>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public Row<ROW_ID_TYPE> next() {
                    return hBase.convert(iterator.next());
                }

                @Override
                public void remove() {
                    throw new RuntimeException("read only");
                }
            };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param forEach
     */
    public void foreach(ForEach<Row<ROW_ID_TYPE>> forEach) {
        for (Row<ROW_ID_TYPE> row : this) {
            forEach.process(row);
        }
    }

    /**
     * @return
     */
    public Where<QUERY_OP_TYPE, ROW_ID_TYPE> where() {
        return new Where<QUERY_OP_TYPE, ROW_ID_TYPE>(this);
    }

    /**
     * @param filter
     */
    protected void setFilter(Filter filter) {
        scan.setFilter(filter);
    }

    /**
     * @param o
     * @return
     */
    protected byte[] toBytes(Object o) {
        return hBase.toBytes(o);
    }

    /**
     * @param whereScanner
     * @param family
     * @param value
     * @return
     */
    protected QueryOps<ROW_ID_TYPE> createWhereClause(
            Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> whereScanner,
            byte[] family, byte[] value) {
        return hBase.createWhereClause(whereScanner, family, value);
    }

    /**
     * @return
     */
    public Select<QUERY_OP_TYPE, ROW_ID_TYPE> select() {
        return new Select<QUERY_OP_TYPE, ROW_ID_TYPE>(this);
    }

    /**
     * @param family
     */
    protected void addFamily(byte[] family) {
        scan.addFamily(family);
    }

    /**
     * @param family
     * @param qualifier
     */
    protected void addColumn(byte[] family, byte[] qualifier) {
        scan.addColumn(family, qualifier);
    }

    /**
     * @param timestamp
     */
    protected void setTimestamp(long timestamp) {
        try {
            scan.setTimeStamp(timestamp);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param minTimestamp
     * @param maxTimestamp
     */
    protected void setTimeRange(long minTimestamp, long maxTimestamp) {
        try {
            scan.setTimeRange(minTimestamp, maxTimestamp);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * version
     */
    public void allVersions() {
        scan.setMaxVersions();
    }
}