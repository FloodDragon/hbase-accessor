package com.xunge.persistence.hbase.api;

/**
 * 事务
 *
 * @author stereo
 */
public interface Transaction {
    public void setRollbackOnly();
}
