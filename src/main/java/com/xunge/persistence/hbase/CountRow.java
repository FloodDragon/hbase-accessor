package com.xunge.persistence.hbase;

import com.xunge.persistence.hbase.api.ForEach;

import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 递增/递减行中的属性值(原子操作)
 *
 * @param <QUERY_OP_TYPE>
 * @param <ROW_ID_TYPE>
 * @author stereo
 */
public class CountRow<QUERY_OP_TYPE extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> {

    private HbaseAccessor<QUERY_OP_TYPE, ROW_ID_TYPE> hBase;
    private byte[] tableName;

    CountRow(HbaseAccessor<QUERY_OP_TYPE, ROW_ID_TYPE> hBase, byte[] tableName) {
        this.hBase = hBase;
        this.tableName = tableName;
    }

    public CountFamilyCol<QUERY_OP_TYPE, ROW_ID_TYPE> row(ROW_ID_TYPE id) {
        return new CountFamilyCol<QUERY_OP_TYPE, ROW_ID_TYPE>(tableName, id,
                this, hBase);
    }

    public <U> CountRow<QUERY_OP_TYPE, ROW_ID_TYPE> rows(Iterable<U> it,
                                                         ForEach<U> process) {
        for (U u : it) {
            process.process(u);
        }
        return this;
    }

    public static class CountFamilyCol<T extends QueryOps<I>, I> {
        private Increment increment;
        private byte[] currentFamily;
        private CountRow<T, I> countRow;
        private HbaseAccessor<T, I> hBase;
        private byte[] tableName;

        CountFamilyCol(byte[] tableName, I id, CountRow<T, I> countRow,
                       HbaseAccessor<T, I> hBase) {
            this.increment = new Increment(hBase.toBytes(id));
            this.countRow = countRow;
            this.hBase = hBase;
            this.tableName = tableName;
        }

        public CountFamilyCol<T, I> family(String name) {
            return family(Bytes.toBytes(name));
        }

        public CountFamilyCol<T, I> family(byte[] name) {
            currentFamily = name;
            return this;
        }

        public CountFamilyCol<T, I> row(I id) {
            return countRow.row(id);
        }

        public CountFamilyCol<T, I> col(String qualifier, long amount) {
            return col(Bytes.toBytes(qualifier), amount);
        }

        public CountFamilyCol<T, I> col(byte[] qualifier, long amount) {
            if (currentFamily == null) {
                throw new RuntimeException("not implemented");
            }
            increment.addColumn(currentFamily, qualifier, amount);
            return this;
        }

        public CountFamilyCol<T, I> flush() {
            this.hBase.flushCount(tableName, increment);
            return this;
        }
    }
}