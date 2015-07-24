package com.xunge.persistence.hbase;

import java.util.Collection;

/**
 * QueryOps扩展
 *
 * @param <QUERY_OP_TYPE>
 * @param <ROW_ID_TYPE>
 * @author stereo
 */
@SuppressWarnings("unchecked")
public class QueryOpsDelegate<QUERY_OP_TYPE extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE>
        extends QueryOps<ROW_ID_TYPE> {

    public QueryOpsDelegate(Where<QUERY_OP_TYPE, ROW_ID_TYPE> whereScanner,
                            byte[] family, byte[] qualifier) {
        super(whereScanner, family, qualifier);
    }

    @Override
    public <U> Where<QUERY_OP_TYPE, ROW_ID_TYPE> betweenEx(U s, U e) {
        return (Where<QUERY_OP_TYPE, ROW_ID_TYPE>) super.betweenEx(s, e);
    }

    @Override
    public <U> Where<QUERY_OP_TYPE, ROW_ID_TYPE> betweenIn(U s, U e) {
        return (Where<QUERY_OP_TYPE, ROW_ID_TYPE>) super.betweenIn(s, e);
    }

    @Override
    public <U> Where<QUERY_OP_TYPE, ROW_ID_TYPE> eq(Collection<U> col) {
        return (Where<QUERY_OP_TYPE, ROW_ID_TYPE>) super.eq(col);
    }

    @Override
    public <U> Where<QUERY_OP_TYPE, ROW_ID_TYPE> eq(U... objs) {
        return (Where<QUERY_OP_TYPE, ROW_ID_TYPE>) super.eq(objs);
    }

    @Override
    public <U> Where<QUERY_OP_TYPE, ROW_ID_TYPE> gt(Collection<U> col) {
        return (Where<QUERY_OP_TYPE, ROW_ID_TYPE>) super.gt(col);
    }

    @Override
    public <U> Where<QUERY_OP_TYPE, ROW_ID_TYPE> gt(U... objs) {
        return (Where<QUERY_OP_TYPE, ROW_ID_TYPE>) super.gt(objs);
    }

    @Override
    public <U> Where<QUERY_OP_TYPE, ROW_ID_TYPE> gte(Collection<U> col) {
        return (Where<QUERY_OP_TYPE, ROW_ID_TYPE>) super.gte(col);
    }

    @Override
    public <U> Where<QUERY_OP_TYPE, ROW_ID_TYPE> gte(U... objs) {
        return (Where<QUERY_OP_TYPE, ROW_ID_TYPE>) super.gte(objs);
    }

    @Override
    public <U> Where<QUERY_OP_TYPE, ROW_ID_TYPE> lt(Collection<U> col) {
        return (Where<QUERY_OP_TYPE, ROW_ID_TYPE>) super.lt(col);
    }

    @Override
    public <U> Where<QUERY_OP_TYPE, ROW_ID_TYPE> lt(U... objs) {
        return (Where<QUERY_OP_TYPE, ROW_ID_TYPE>) super.lt(objs);
    }

    @Override
    public <U> Where<QUERY_OP_TYPE, ROW_ID_TYPE> lte(Collection<U> col) {
        return (Where<QUERY_OP_TYPE, ROW_ID_TYPE>) super.lte(col);
    }

    @Override
    public <U> Where<QUERY_OP_TYPE, ROW_ID_TYPE> lte(U... objs) {
        return (Where<QUERY_OP_TYPE, ROW_ID_TYPE>) super.lte(objs);
    }

    @Override
    public <U> Where<QUERY_OP_TYPE, ROW_ID_TYPE> contains(String pattern) {
        return (Where<QUERY_OP_TYPE, ROW_ID_TYPE>) super.contains(pattern);
    }

    @Override
    public Where<QUERY_OP_TYPE, ROW_ID_TYPE> match(String pattern) {
        return (Where<QUERY_OP_TYPE, ROW_ID_TYPE>) super.match(pattern);
    }

    @Override
    public <U> Where<QUERY_OP_TYPE, ROW_ID_TYPE> ne(Collection<U> col) {
        return (Where<QUERY_OP_TYPE, ROW_ID_TYPE>) super.ne(col);
    }

    @Override
    public <U> Where<QUERY_OP_TYPE, ROW_ID_TYPE> ne(U... objs) {
        return (Where<QUERY_OP_TYPE, ROW_ID_TYPE>) super.ne(objs);
    }
}
