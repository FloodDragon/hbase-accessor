package com.xunge.persistence.hbase;

import java.util.Date;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.xunge.persistence.hbase.api.ForEach;

/**
 * 存储/更新一行数据
 *
 * @param <QUERY_OP_TYPE>
 * @param <ROW_ID_TYPE>
 * @author stereo
 */
public class SaveRow<QUERY_OP_TYPE extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> {

    private HbaseAccessor<QUERY_OP_TYPE, ROW_ID_TYPE> hBase;
    private byte[] tableName;

    SaveRow(HbaseAccessor<QUERY_OP_TYPE, ROW_ID_TYPE> hBase, byte[] tableName) {
        this.hBase = hBase;
        this.tableName = tableName;
    }

    public SaveFamilyCol<QUERY_OP_TYPE, ROW_ID_TYPE> row(ROW_ID_TYPE id) {
        return new SaveFamilyCol<QUERY_OP_TYPE, ROW_ID_TYPE>(tableName, id,
                this, hBase);
    }

    public <U> SaveRow<QUERY_OP_TYPE, ROW_ID_TYPE> rows(Iterable<U> it,
                                                        ForEach<U> process) {
        for (U u : it) {
            process.process(u);
        }
        return this;
    }

    public SaveRow<QUERY_OP_TYPE, ROW_ID_TYPE> flush() {
        hBase.flush(tableName);
        return this;
    }

    public static class SaveFamilyCol<T extends QueryOps<I>, I> {

        private Put put;
        private byte[] currentFamily;
        private SaveRow<T, I> saveRow;
        private HbaseAccessor<T, I> hBase;
        private byte[] tableName;

        SaveFamilyCol(byte[] tableName, I id, SaveRow<T, I> saveRow,
                      HbaseAccessor<T, I> hBase) {
            this.put = new Put(hBase.toBytes(id));
            this.saveRow = saveRow;
            this.hBase = hBase;
            this.tableName = tableName;
        }

        public SaveFamilyCol<T, I> family(String name) {
            return family(Bytes.toBytes(name));
        }

        public SaveFamilyCol<T, I> family(byte[] name) {
            currentFamily = name;
            return this;
        }

        public SaveFamilyCol<T, I> col(String qualifier, Object o) {
            return col(Bytes.toBytes(qualifier), o);
        }

        public SaveFamilyCol<T, I> col(byte[] qualifier, Object o) {
            return col(qualifier, o, (Long) null);
        }

        public SaveFamilyCol<T, I> col(String qualifier, Object o,
                                       Long timestamp) {
            return col(Bytes.toBytes(qualifier), o, timestamp);
        }

        public SaveFamilyCol<T, I> col(byte[] qualifier, Object o,
                                       Long timestamp) {
            if (currentFamily == null) {
                throw new RuntimeException("not implemented");
            }
            if (o == null) {
                return this;
            }
            if (timestamp == null) {
                put.add(currentFamily, qualifier, hBase.toBytes(o));
            } else {
                put.add(currentFamily, qualifier, timestamp, hBase.toBytes(o));
            }
            return this;
        }

        public SaveFamilyCol<T, I> col(String qualifier, Object o,
                                       Date timestamp) {
            return col(Bytes.toBytes(qualifier), o, timestamp);
        }

        public SaveFamilyCol<T, I> col(byte[] qualifier, Object o,
                                       Date timestamp) {
            if (timestamp == null) {
                return col(qualifier, o);
            }
            return col(qualifier, o, timestamp.getTime());
        }

        public SaveFamilyCol<T, I> row(I id) {
            return saveRow.row(id);
        }

        public SaveFamilyCol<T, I> commit() {
            if (put != null) {
                this.hBase.savePut(tableName, put);
                put = null;
            }
            return this;
        }

        public SaveFamilyCol<T, I> flush() {
            commit();
            hBase.flush(tableName);
            return this;
        }
    }
}