package com.xunge.persistence.hbase.api;

/**
 * 数据列
 *
 * @author stereo
 */
public interface Column {

    String qualifier();

    <U> U value(Class<U> c);
}
