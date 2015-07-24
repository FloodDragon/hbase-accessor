package com.xunge.persistence.hbase;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;

import com.xunge.persistence.hbase.api.ForEach;

/**
 * 删除KeyRow
 *
 * @param <QUERY_OP_TYPE>
 * @param <ROW_ID_TYPE>
 * @author stereo
 */
public class DeletedRow<QUERY_OP_TYPE extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> {

    private HbaseAccessor<QUERY_OP_TYPE, ROW_ID_TYPE> hBase;
    private byte[] tableName;

    DeletedRow(HbaseAccessor<QUERY_OP_TYPE, ROW_ID_TYPE> hBase, byte[] tableName) {
        this.hBase = hBase;
        this.tableName = tableName;
    }

    public DeletedRowFamily<QUERY_OP_TYPE, ROW_ID_TYPE> row(ROW_ID_TYPE id) {
        return new DeletedRowFamily<QUERY_OP_TYPE, ROW_ID_TYPE>(tableName, id,
                this, hBase);
    }

    public DeletedRow<QUERY_OP_TYPE, ROW_ID_TYPE> rows(Iterable<ROW_ID_TYPE> it) {
        return rows(it, null);
    }

    public DeletedRow<QUERY_OP_TYPE, ROW_ID_TYPE> rows(
            Iterable<ROW_ID_TYPE> it,
            ForEach<DeletedRowFamily<QUERY_OP_TYPE, ROW_ID_TYPE>> forEach) {
        for (ROW_ID_TYPE id : it) {
            DeletedRowFamily<QUERY_OP_TYPE, ROW_ID_TYPE> deleteRowFamily = row(id);
            if (forEach != null) {
                forEach.process(deleteRowFamily);
            }
        }
        return this;
    }

    public DeletedRow<QUERY_OP_TYPE, ROW_ID_TYPE> flush() {
        hBase.flush(tableName);
        return this;
    }

    public static class DeletedRowFamily<T extends QueryOps<I>, I> {

        private HbaseAccessor<T, I> hBase;
        private byte[] tableName;
        private Delete delete;
        private DeletedRow<T, I> deleteRow;

        DeletedRowFamily(byte[] tableName, I id, DeletedRow<T, I> deleteRow,
                         HbaseAccessor<T, I> hBase) {
            this.delete = new Delete(hBase.toBytes(id));
            this.deleteRow = deleteRow;
            this.hBase = hBase;
            this.tableName = tableName;
            this.hBase.saveDelete(this.tableName, delete);
        }

        public DeletedRowFamilyColumn<T, I> family(String name) {
            return family(Bytes.toBytes(name));
        }

        public DeletedRowFamilyColumn<T, I> family(byte[] name) {
            return new DeletedRowFamilyColumn<T, I>(hBase, tableName, name,
                    delete, deleteRow);
        }

        public DeletedRowFamily<T, I> deleteFamily(String name) {
            return deleteFamily(Bytes.toBytes(name));
        }

        public DeletedRowFamily<T, I> deleteFamily(byte[] name) {
            if (name == null)
                throw new RuntimeException("not implemented");
            else
                delete.deleteFamily(name);
            return this;
        }

        public DeletedRowFamily<T, I> row(I id) {
            return deleteRow.row(id);
        }

        public DeletedRowFamily<T, I> flush() {
            hBase.flush(tableName);
            return this;
        }
    }

    public static class DeletedRowFamilyColumn<T extends QueryOps<I>, I> {

        private HbaseAccessor<T, I> hBase;
        private byte[] tableName;
        private byte[] currentFamily;
        private Delete delete;
        private DeletedRow<T, I> deleteRow;

        DeletedRowFamilyColumn(HbaseAccessor<T, I> hBase, byte[] tableName,
                               byte[] currentFamily, Delete delete, DeletedRow<T, I> deleteRow) {
            this.hBase = hBase;
            this.tableName = tableName;
            this.currentFamily = currentFamily;
            this.delete = delete;
            this.deleteRow = deleteRow;
        }

        public DeletedRowFamilyColumn<T, I> family(String name) {
            return family(Bytes.toBytes(name));
        }

        public DeletedRowFamilyColumn<T, I> family(byte[] name) {
            return new DeletedRowFamilyColumn<T, I>(hBase, tableName, name,
                    delete, deleteRow);
        }

        public DeletedRowFamilyColumn<T, I> col(String name) {
            return col(Bytes.toBytes(name));
        }

        public DeletedRowFamilyColumn<T, I> col(byte[] name) {
            if (currentFamily == null) {
                throw new RuntimeException("not implemented");
            } else {
                delete.deleteColumn(currentFamily, name);
            }
            return this;
        }

        public DeletedRowFamily<T, I> row(I id) {
            return deleteRow.row(id);
        }

        public DeletedRowFamilyColumn<T, I> flush() {
            hBase.flush(tableName);
            return this;
        }
    }
}