package com.xunge.persistence.hbase.api;

/**
 * 类型转换
 *
 * @param <T>
 * @author stereo
 */
public interface TypeConverter<T> {

    Class<?>[] getTypes();

    byte[] toBytes(T t);

    T fromBytes(byte[] t);
}
