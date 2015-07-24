package com.hbaseaccessor.test;

import com.xunge.persistence.hbase.HbaseAccessor;
import com.xunge.persistence.hbase.QueryOps;
import com.xunge.persistence.hbase.api.Column;
import com.xunge.persistence.hbase.api.ForEach;
import com.xunge.persistence.hbase.api.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 * Created by liujing11 on 2015/7/24.
 */
public class HbaseAccessorTest {

    /**
     * 简单测试和使用
     *
     * @param args
     */
    public static void main(String[] args) {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "xxx.xxx.x.xx");
        configuration.set("hbase.master", "xxx.xxx.x.xx:60000");

        final HbaseAccessor<QueryOps<String>, String> accessor = new HbaseAccessor<QueryOps<String>, String>(
                String.class, configuration);
        //用法类似sql

        //表定义TableAdmin类实现
        //插入和修改
        accessor.table("表名").save().row("行健").family("列族").col("限定符-列", "值").flush();

        //删除
        accessor.table("表名").delete().row("行健").family("列族").col("列").flush();

        //查询有二种
        accessor.table("表名").fetch().row("行健").family("列族").foreach(new ForEach<Column>() {
            @Override
            public void process(Column column) {
                //查询把....
                column.qualifier();//key
                column.value(String.class);//value
            }
        });//最快

        //条件查询 eq(等于) ne(不等于) lt(小鱼) gt(大雨) lte(小鱼等于) gte(大于等于) contains包含 match匹配(模糊查询) betweenIn betweenEx 括号是lp() rp()
        //为了快速定位scan()参数里可已传startkey endkey
        accessor.table("表名").scan().where().family("列族").col("字段1").eq("参数1").col("字段2").eq("参数2").foreach(new ForEach<Row<String>>() {
            @Override
            public void process(Row<String> stringRow) {
                //这一列就是条件查询的一列就可以迭代了
                stringRow.family("列族").foreach(new ForEach<Column>() {
                    @Override
                    public void process(Column column) {
                        //这个是查询列族里面的列
                    }
                });
            }
        });
        //上面代码 = select * from table where 条件查询 加分区
        //简单查询
        accessor.table("表名").scan().select().family("列族").col("列");
        //用于分页
        accessor.table("表名").scan().where().family("列族").limit(5).lp();
        //以上是简单介绍，使用可以灵活搭配
    }
}
