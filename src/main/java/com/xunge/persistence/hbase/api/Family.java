package com.xunge.persistence.hbase.api;

import java.util.List;
import java.util.NavigableMap;

/**
 * 数据列族
 *
 * @author stereo
 */
public interface Family {

    <U> U value(String qualifier, Class<U> c);

    <U> U value(byte[] qualifier, Class<U> c);

    <U> NavigableMap<Long, U> values(String qualifier, Class<U> c);

    <U> NavigableMap<Long, U> values(byte[] qualifier, Class<U> c);

    <U> List<U> valuesAscTimestamp(String qualifier, Class<U> c);

    <U> List<U> valuesAscTimestamp(byte[] qualifier, Class<U> c);

    <U> List<U> valuesDescTimestamp(String qualifier, Class<U> c);

    <U> List<U> valuesDescTimestamp(byte[] qualifier, Class<U> c);

    void foreach(ForEach<Column> forEach);
}
