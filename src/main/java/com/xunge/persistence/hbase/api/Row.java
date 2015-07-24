package com.xunge.persistence.hbase.api;

import java.util.List;
import java.util.NavigableMap;

/**
 * @param <ROW_ID_TYPE>
 * @author stereo
 */
public interface Row<ROW_ID_TYPE> {

    ROW_ID_TYPE getId();

    Family family(String family);

    Family family(byte[] family);

    <U> U value(String family, String qualifier, Class<U> c);

    <U> U value(byte[] family, byte[] qualifier, Class<U> c);

    <U> NavigableMap<Long, U> values(String family, String qualifier, Class<U> c);

    <U> NavigableMap<Long, U> values(byte[] family, byte[] qualifier, Class<U> c);

    <U> List<U> valuesAscTimestamp(String family, String qualifier, Class<U> c);

    <U> List<U> valuesAscTimestamp(byte[] family, byte[] qualifier, Class<U> c);

    <U> List<U> valuesDescTimestamp(String family, String qualifier, Class<U> c);

    <U> List<U> valuesDescTimestamp(byte[] family, byte[] qualifier, Class<U> c);
}