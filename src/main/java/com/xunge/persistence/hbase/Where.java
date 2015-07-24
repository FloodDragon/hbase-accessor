package com.xunge.persistence.hbase;

import java.util.Iterator;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.xunge.persistence.hbase.api.ForEach;
import com.xunge.persistence.hbase.api.Row;

/**
 * 条件查询
 *
 * @param <QUERY_OP_TYPE>
 * @param <ROW_ID_TYPE>
 * @author stereo
 */
public class Where<QUERY_OP_TYPE extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE>
        implements Iterable<Row<ROW_ID_TYPE>> {

    protected enum BOOL_EXP {
        OR, AND
    }

    private Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scanner;
    private byte[] family;
    private QueryContext context = new QueryContext();

    Where(Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scanner) {
        this.scanner = scanner;
    }

    /**
     * @param name
     * @return
     */
    public Where<QUERY_OP_TYPE, ROW_ID_TYPE> family(String name) {
        return family(Bytes.toBytes(name));
    }

    public Where<QUERY_OP_TYPE, ROW_ID_TYPE> family(byte[] name) {
        family = name;
        return this;
    }

    /**
     * @param name
     * @return
     */
    public QUERY_OP_TYPE col(String name) {
        return col(Bytes.toBytes(name));
    }

    /**
     * (QUERY_OP_TYPE extends QueryOps)
     *
     * @param name
     * @return
     */
    @SuppressWarnings("unchecked")
    public QUERY_OP_TYPE col(byte[] name) {
        return (QUERY_OP_TYPE) scanner.createWhereClause(this, family, name);
    }

    /**
     * @param forEach
     */
    public void foreach(ForEach<Row<ROW_ID_TYPE>> forEach) {
        Filter filter = context.getResultingFilter();
        scanner.setFilter(filter);
        scanner.foreach(forEach);
    }

    /**
     * @param filter
     * @return
     */
    public Where<QUERY_OP_TYPE, ROW_ID_TYPE> filter(Filter filter) {
        return addFilter(filter);
    }

    /**
     * @return
     */
    public Where<QUERY_OP_TYPE, ROW_ID_TYPE> or() {
        context.addBooleanExp(BOOL_EXP.OR);
        return this;
    }

    /**
     * @return
     */
    public Where<QUERY_OP_TYPE, ROW_ID_TYPE> and() {
        context.addBooleanExp(BOOL_EXP.AND);
        return this;
    }

    /**
     * @return
     */
    public Where<QUERY_OP_TYPE, ROW_ID_TYPE> lp() {
        context.down();
        return this;
    }

    /**
     * @return
     */
    public Where<QUERY_OP_TYPE, ROW_ID_TYPE> rp() {
        context.up();
        return this;
    }

    /**
     * @param size
     * @return
     */
    public Where<QUERY_OP_TYPE, ROW_ID_TYPE> limit(long size) {
        PageFilter pageFilter = new PageFilter(size);
        return addFilter(pageFilter);
    }

    /**
     * @param prefix
     * @return
     */
    public Where<QUERY_OP_TYPE, ROW_ID_TYPE> prefix(String prefix) {
        return prefix(Bytes.toBytes(prefix));
    }

    /**
     * @param prefix
     * @return
     */
    public Where<QUERY_OP_TYPE, ROW_ID_TYPE> prefix(byte[] prefix) {
        PrefixFilter prefixFileFilter = new PrefixFilter(prefix);
        return addFilter(prefixFileFilter);
    }

    /**
     * @param o
     * @return
     */
    protected byte[] toBytes(Object o) {
        return scanner.toBytes(o);
    }

    /**
     * @param filter
     * @return
     */
    protected Where<QUERY_OP_TYPE, ROW_ID_TYPE> addFilter(Filter filter) {
        context.addFilter(filter);
        return this;
    }

    @Override
    public Iterator<Row<ROW_ID_TYPE>> iterator() {
        return scanner.iterator();
    }

    public Where<QUERY_OP_TYPE, ROW_ID_TYPE> allVersions() {
        scanner.allVersions();
        return this;
    }
}