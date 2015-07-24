package com.xunge.persistence.hbase.api;

import com.xunge.persistence.hbase.CountRow;
import com.xunge.persistence.hbase.DeletedRow;
import com.xunge.persistence.hbase.FetchRow;
import com.xunge.persistence.hbase.QueryOps;
import com.xunge.persistence.hbase.SaveRow;
import com.xunge.persistence.hbase.Scanner;

/**
 * @param <QUERY_OP_TYPE>
 * @param <ROW_ID_TYPE>
 * @author stereo
 */
public interface Table<QUERY_OP_TYPE extends QueryOps<ROW_ID_TYPE>, ROW_ID_TYPE> {

    FetchRow<ROW_ID_TYPE> fetch();

    Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scan();

    SaveRow<QUERY_OP_TYPE, ROW_ID_TYPE> save();

    DeletedRow<QUERY_OP_TYPE, ROW_ID_TYPE> delete();

    CountRow<QUERY_OP_TYPE, ROW_ID_TYPE> count();

    Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scan(ROW_ID_TYPE startId);

    Scanner<QUERY_OP_TYPE, ROW_ID_TYPE> scan(ROW_ID_TYPE startId,
                                             ROW_ID_TYPE endId);
}