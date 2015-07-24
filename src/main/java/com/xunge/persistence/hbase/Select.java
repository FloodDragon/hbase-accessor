package com.xunge.persistence.hbase;

import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.hbase.util.Bytes;

import com.xunge.persistence.hbase.api.ForEach;
import com.xunge.persistence.hbase.api.Row;

/**
 * 选择查询
 *
 * @param <QUERY_OP_TYPE>
 * @param <ROW_ID_TYPE>
 * @author stereo
 */
public class Select<QUERY_OP_TYPE extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE>
        implements Iterable<Row<ROW_ID_TYPE>> {

    private byte[] family;
    private Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scanner;

    Select(Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scanner) {
        this.scanner = scanner;
    }

    /**
     * 创建Where
     *
     * @return
     */
    public Where<QUERY_OP_TYPE, ROW_ID_TYPE> where() {
        return new Where<QUERY_OP_TYPE, ROW_ID_TYPE>(scanner);
    }

    /**
     * @param forEach
     */
    public void foreach(ForEach<Row<ROW_ID_TYPE>> forEach) {
        scanner.foreach(forEach);
    }

    /**
     * @param name
     * @return
     */
    public Select<QUERY_OP_TYPE, ROW_ID_TYPE> family(String name) {
        return family(Bytes.toBytes(name));
    }

    /**
     * @param name
     * @return
     */
    public Select<QUERY_OP_TYPE, ROW_ID_TYPE> col(String name) {
        return col(Bytes.toBytes(name));
    }

    /**
     * @param name
     * @return
     */
    public Select<QUERY_OP_TYPE, ROW_ID_TYPE> family(byte[] name) {
        family = name;
        scanner.addFamily(name);
        return this;
    }

    /**
     * @param name
     * @return
     */
    public Select<QUERY_OP_TYPE, ROW_ID_TYPE> col(byte[] name) {
        scanner.addColumn(family, name);
        return this;
    }

    @Override
    public Iterator<Row<ROW_ID_TYPE>> iterator() {
        return scanner.iterator();
    }

    /**
     * @param timestamp
     * @return
     */
    public Select<QUERY_OP_TYPE, ROW_ID_TYPE> timestamp(long timestamp) {
        scanner.setTimestamp(timestamp);
        return this;
    }

    /**
     * @param minTimestamp
     * @param maxTimestamp
     * @return
     */
    public Select<QUERY_OP_TYPE, ROW_ID_TYPE> timerange(long minTimestamp,
                                                        long maxTimestamp) {
        scanner.setTimeRange(minTimestamp, maxTimestamp);
        return this;
    }

    /**
     * @param timestamp
     * @return
     */
    public Select<QUERY_OP_TYPE, ROW_ID_TYPE> timestamp(Date timestamp) {
        scanner.setTimestamp(timestamp.getTime());
        return this;
    }

    /**
     * @param minTimestamp
     * @param maxTimestamp
     * @return
     */
    public Select<QUERY_OP_TYPE, ROW_ID_TYPE> timerange(Date minTimestamp,
                                                        Date maxTimestamp) {
        scanner.setTimeRange(minTimestamp.getTime(), maxTimestamp.getTime());
        return this;
    }

    public Select<QUERY_OP_TYPE, ROW_ID_TYPE> allVersions() {
        scanner.allVersions();
        return this;
    }
}