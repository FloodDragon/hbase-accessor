package com.xunge.persistence.hbase;

import java.util.Map;
import java.util.NavigableSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.xunge.persistence.hbase.api.Row;

/**
 * 简便查询
 *
 * @param <ROW_ID_TYPE>
 * @author stereo
 */
public class FetchRow<ROW_ID_TYPE> {

    private static final Log LOG = LogFactory.getLog(FetchRow.class);
    private byte[] currentFamily;
    private Get get;
    private byte[] tableName;
    private HbaseAccessor<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> hBase;
    private Result result;

    FetchRow(HbaseAccessor<? extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> hBase,
             byte[] tableName) {
        this.hBase = hBase;
        this.tableName = tableName;
    }

    public Row<ROW_ID_TYPE> row(ROW_ID_TYPE id) {
        get = newGet(id);
        fetch();
        if (result.getRow() == null) {
            return null;
        }
        return new ResultRow<ROW_ID_TYPE>(hBase, result);
    }

    public FetchRow<ROW_ID_TYPE> select() {
        return this;
    }

    public FetchRow<ROW_ID_TYPE> family(String family) {
        return family(Bytes.toBytes(family));
    }

    public FetchRow<ROW_ID_TYPE> family(byte[] family) {
        currentFamily = family;
        get.addFamily(currentFamily);
        return this;
    }

    public FetchRow<ROW_ID_TYPE> col(String name) {
        return col(Bytes.toBytes(name));
    }

    public FetchRow<ROW_ID_TYPE> col(byte[] name) {
        get.addColumn(currentFamily, name);
        return this;
    }

    private void fetch() {
        if (result == null) {
            LOG.debug("Fetching row with id [" + Bytes.toString(get.getRow())
                    + "]");
            result = hBase.getResult(tableName, get);
        }
    }

    private Get newGet(ROW_ID_TYPE id) {
        Get newGet = new Get(hBase.toBytes(id));
        Map<byte[], NavigableSet<byte[]>> familyMap = get.getFamilyMap();
        for (byte[] family : familyMap.keySet()) {
            NavigableSet<byte[]> qualifiers = familyMap.get(family);
            if (qualifiers == null) {
                newGet.addFamily(family);
            } else {
                for (byte[] qualifier : qualifiers) {
                    newGet.addColumn(family, qualifier);
                }
            }
        }
        return newGet;
    }
}