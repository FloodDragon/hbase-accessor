package com.xunge.persistence.hbase.api;

/**
 * 迭代器
 *
 * @param <T>
 * @author stereo
 */
public interface ForEach<T> {
    void process(T t);
}
