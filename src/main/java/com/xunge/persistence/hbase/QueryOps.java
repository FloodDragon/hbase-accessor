package com.xunge.persistence.hbase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;

/**
 * Where选项 相当Sql中聚合函数(少用)
 *
 * @param <ROW_ID_TYPE>
 * @author stereo
 */
public class QueryOps<ROW_ID_TYPE> {

    protected Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> whereScanner;
    protected byte[] qualifier;
    protected byte[] family;

    public QueryOps(
            Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> whereScanner,
            byte[] family, byte[] qualifier) {
        this.whereScanner = whereScanner;
        this.family = family;
        this.qualifier = qualifier;
    }

    public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> eq(U... objs) {
        return objs == null ? whereScanner : eq(Arrays.asList(objs));
    }

    public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> ne(U... objs) {
        return objs == null ? whereScanner : ne(Arrays.asList(objs));
    }

    public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> lt(U... objs) {
        return objs == null ? whereScanner : lt(Arrays.asList(objs));
    }

    public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> gt(U... objs) {
        return objs == null ? whereScanner : gt(Arrays.asList(objs));
    }

    public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> lte(
            U... objs) {
        return objs == null ? whereScanner : lte(Arrays.asList(objs));
    }

    public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> gte(
            U... objs) {
        return objs == null ? whereScanner : gte(Arrays.asList(objs));
    }

    /**
     * 等于
     *
     * @param col
     * @return
     */
    public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> eq(
            Collection<U> col) {
        return getCommonFilter(col, CompareOp.EQUAL, true);
    }

    /**
     * 不等于
     *
     * @param col
     * @return
     */

    public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> ne(
            Collection<U> col) {
        return getCommonFilter(col, CompareOp.NOT_EQUAL, false);
    }

    /**
     * 小于
     *
     * @param col
     * @return
     */
    public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> lt(
            Collection<U> col) {
        return getCommonFilter(col, CompareOp.LESS, true);
    }

    /**
     * 大于
     *
     * @param col
     * @return
     */
    public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> gt(
            Collection<U> col) {
        return getCommonFilter(col, CompareOp.GREATER, true);
    }

    /**
     * 小于等于
     *
     * @param col
     * @return
     */
    public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> lte(
            Collection<U> col) {
        return getCommonFilter(col, CompareOp.LESS_OR_EQUAL, true);
    }

    /**
     * 大于等于
     *
     * @param col
     * @return
     */
    public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> gte(
            Collection<U> col) {
        return getCommonFilter(col, CompareOp.GREATER_OR_EQUAL, true);
    }

    /**
     * 包含
     *
     * @param pattern
     * @return
     */
    public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> contains(
            String pattern) {
        SubstringComparator comparator = new SubstringComparator(pattern);
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                family, qualifier, CompareOp.EQUAL, comparator);
        singleColumnValueFilter.setFilterIfMissing(true);
        return whereScanner.addFilter(singleColumnValueFilter);
    }

    /**
     * 匹配
     *
     * @param regexPattern
     * @return
     */
    public Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> match(
            String regexPattern) {
        RegexStringComparator regexStringComparator = new RegexStringComparator(
                regexPattern);
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                family, qualifier, CompareOp.EQUAL, regexStringComparator);
        singleColumnValueFilter.setFilterIfMissing(true);
        return whereScanner.addFilter(singleColumnValueFilter);
    }

    /**
     * betweenIn
     *
     * @param s
     * @param e
     * @return
     */
    public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> betweenIn(
            U s, U e) {
        Filter leftFilter = getSingleValueEqualFilter(whereScanner.toBytes(s),
                CompareOp.GREATER_OR_EQUAL, true);
        Filter rightFilter = getSingleValueEqualFilter(whereScanner.toBytes(e),
                CompareOp.LESS_OR_EQUAL, true);
        return whereScanner.addFilter(new FilterList(Operator.MUST_PASS_ALL,
                Arrays.asList(leftFilter, rightFilter)));
    }

    /**
     * betweenEx
     *
     * @param s
     * @param e
     * @return
     */
    public <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> betweenEx(
            U s, U e) {
        Filter leftFilter = getSingleValueEqualFilter(whereScanner.toBytes(s),
                CompareOp.GREATER, true);
        Filter rightFilter = getSingleValueEqualFilter(whereScanner.toBytes(e),
                CompareOp.LESS, true);
        return whereScanner.addFilter(new FilterList(Operator.MUST_PASS_ALL,
                Arrays.asList(leftFilter, rightFilter)));
    }

    /**
     * 简单Value过滤
     *
     * @param value
     * @param compareOp
     * @param filterIfMissing
     * @return
     */
    private Filter getSingleValueEqualFilter(byte[] value, CompareOp compareOp,
                                             boolean filterIfMissing) {
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                family, qualifier, compareOp, value);
        singleColumnValueFilter.setFilterIfMissing(filterIfMissing);
        return singleColumnValueFilter;
    }

    /**
     * 一般过滤
     *
     * @param col
     * @param compareOp
     * @param filterIfMissing
     * @return
     */
    protected <U> Where<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> getCommonFilter(
            Collection<U> col, CompareOp compareOp, boolean filterIfMissing) {
        if (col == null || col.size() == 0) {
            return whereScanner;
        }
        if (col.size() == 1) {
            byte[] value = whereScanner.toBytes(col.iterator().next());
            return whereScanner.addFilter(getSingleValueEqualFilter(value,
                    compareOp, filterIfMissing));
        }
        List<Filter> list = new ArrayList<Filter>();
        for (Object o : col) {
            byte[] value = whereScanner.toBytes(o);
            list.add(getSingleValueEqualFilter(value, compareOp,
                    filterIfMissing));
        }
        return whereScanner.addFilter(new FilterList(Operator.MUST_PASS_ONE,
                list));
    }
}