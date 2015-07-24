package com.xunge.persistence.hbase;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 表管理
 *
 * @author stereo
 */
public class TableAdmin {

    private static final Log LOG = LogFactory.getLog(TableAdmin.class);
    private HBaseAdmin admin;
    private byte[] tableName;
    private HTableDescriptor desc;

    TableAdmin(byte[] tableName, Configuration conf) {
        this.tableName = tableName;
        try {
            admin = new HBaseAdmin(conf);
            if (!admin.tableExists(tableName)) {
                desc = new HTableDescriptor(this.tableName);
                admin.createTable(desc);
            } else {
                LOG.info("table [" + tableName + "] already exists");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public FamilyAdmin family(String name) {
        return family(Bytes.toBytes(name));
    }

    /**
     * 添加列族
     *
     * @param name
     * @return
     */
    public FamilyAdmin family(byte[] name) {
        try {
            desc = admin.getTableDescriptor(tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (desc.getFamiliesKeys().contains(name)) {
            LOG.info("family [" + name + "] already exists");
            return new FamilyAdmin(this, admin, tableName, desc, name);
        }
        try {
            LOG.info("Adding family [" + Bytes.toString(name) + "] to table ["
                    + Bytes.toString(tableName) + "]");
            admin.disableTable(tableName);
            HColumnDescriptor family = new HColumnDescriptor(name);
            desc.addFamily(family);
            admin.modifyTable(tableName, desc);
            admin.enableTable(tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new FamilyAdmin(this, admin, tableName, desc, name);
    }

    /**
     * 列族管理
     *
     * @author stereo
     */
    public static class FamilyAdmin {

        private TableAdmin tableAdmin;
        private HBaseAdmin admin;
        private byte[] tableName;
        private HTableDescriptor desc;
        private byte[] familyName;

        FamilyAdmin(TableAdmin tableAdmin, HBaseAdmin admin, byte[] tableName,
                    HTableDescriptor desc, byte[] familyName) {
            this.tableAdmin = tableAdmin;
            this.admin = admin;
            this.tableName = tableName;
            this.desc = desc;
            this.familyName = familyName;
        }

        public FamilyAdmin family(String name) {
            return tableAdmin.family(name);
        }

        public FamilyAdmin family(byte[] name) {
            return tableAdmin.family(name);
        }

        public FamilyAdmin inMemory() {
            HColumnDescriptor columnDescriptor = desc.getFamily(familyName);
            if (!columnDescriptor.isInMemory()) {
                LOG.info("Setting family [" + Bytes.toString(familyName)
                        + "] to be in memory family.");
                disableTable();
                columnDescriptor.setInMemory(true);
                desc.addFamily(columnDescriptor);
                return modifyAndEnable();
            }
            LOG.info("Family [" + Bytes.toString(familyName)
                    + "] is in memory family.");
            return this;
        }

        public FamilyAdmin enableBloomFilter(StoreFile.BloomType bloomType) {
            HColumnDescriptor columnDescriptor = desc.getFamily(familyName);
            if (columnDescriptor.getBloomFilterType() == StoreFile.BloomType.NONE) {
                LOG.info("Enable Bloom Filter for family ["
                        + Bytes.toString(familyName) + "].");
                disableTable();
                columnDescriptor.setBloomFilterType(bloomType);
                desc.addFamily(columnDescriptor);
                return modifyAndEnable();
            }
            LOG.info("Bloom Filter for family [" + Bytes.toString(familyName)
                    + "] enabled.");
            return this;
        }

        public FamilyAdmin disableBlockCache() {
            HColumnDescriptor columnDescriptor = desc.getFamily(familyName);
            if (columnDescriptor.isBlockCacheEnabled()) {
                LOG.info("Disable Bloom Cache for family ["
                        + Bytes.toString(familyName) + "].");
                disableTable();
                columnDescriptor.setBlockCacheEnabled(false);
                desc.addFamily(columnDescriptor);
                return modifyAndEnable();
            }
            LOG.info("Bloom Cache for family [" + Bytes.toString(familyName)
                    + "] disabled.");
            return this;
        }

        private void disableTable() {
            try {
                admin.disableTable(tableName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private FamilyAdmin modifyAndEnable() {
            try {
                admin.modifyTable(tableName, desc);
                admin.enableTable(tableName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        // family.setBlocksize(s);
        // family.setBloomfilter(onOff);
        // family.setCompressionType(type);
        // family.setInMemory(inMemory);
        // family.setMapFileIndexInterval(interval);
        // family.setTimeToLive(timeToLive);
    }
}