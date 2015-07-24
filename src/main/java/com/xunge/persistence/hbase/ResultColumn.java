package com.xunge.persistence.hbase;

import org.apache.hadoop.hbase.util.Bytes;

import com.xunge.persistence.hbase.api.Column;

public class ResultColumn implements Column {

    private byte[] qualifier;
    private byte[] value;
    private HbaseAccessor<?, ?> hBase;

    public ResultColumn(HbaseAccessor<?, ?> hBase, byte[] qualifier,
                        byte[] value) {
        this.hBase = hBase;
        this.qualifier = qualifier;
        this.value = value;
    }

    @Override
    public String qualifier() {
        return Bytes.toString(qualifier);
    }

    @Override
    public <U> U value(Class<U> c) {
        return hBase.fromBytes(value, c);
    }
}