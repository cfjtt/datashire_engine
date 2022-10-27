package com.eurlanda.datashire.engine.java;

import com.eurlanda.datashire.engine.entity.TDataSource;
import com.eurlanda.datashire.engine.util.DateUtil;
import com.eurlanda.datashire.engine.util.cool.JList;
import com.eurlanda.datashire.enumeration.DataBaseType;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.phoenix.compile.QueryPlan;
import org.junit.Test;

import java.sql.Connection;
import java.text.ParseException;
import java.util.List;

import static com.eurlanda.datashire.engine.spark.DatabaseUtils.getConnection;
import static com.eurlanda.datashire.engine.spark.DatabaseUtils.getQueryPlan;

/**
 * Created by zhudebin on 15-1-13.
 */
public class HessianTest {

    @Test
    public void testSe() throws ParseException {
        TDataSource ds = new TDataSource();
        ds.setType(DataBaseType.HBASE_PHOENIX);
        ds.setHost("e223");
        ds.setPort(2181);
        ds.setTableName("NJ_LOAD_DATA_06");
        ds.setFilter("JGSJ BETWEEN TO_DATE('2014-06-01 00:00:13', 'yyyy-MM-dd HH:mm:ss', 'GMT+8') " +
                "AND TO_DATE('2014-06-01 00:11:13', 'yyyy-MM-dd HH:mm:ss', 'GMT+8') AND KKMC='柘汪新老204路口北' AND HPHM IS NOT NULL");

        Connection con = getConnection(ds);
        /*QueryPlan qp = getQueryPlan(con, "select HPHM from NJ_LOAD_DATA_06 where " + "JGSJ BETWEEN TO_DATE('2014-06-01 00:00:13', 'yyyy-MM-dd HH:mm:ss', 'GMT+8') " +
                "AND TO_DATE('2014-06-01 00:11:13', 'yyyy-MM-dd HH:mm:ss', 'GMT+8') AND KKMC='柘汪新老204路口北' AND HPHM IS NOT NULL", null);
        */
        QueryPlan qp = getQueryPlan(con, "select HPHM from NJ_LOAD_DATA_06 where " + "JGSJ BETWEEN TO_DATE('2014-06-01 00:00:13', 'yyyy-MM-dd HH:mm:ss', 'GMT+8') " +
                "AND ? AND KKMC='柘汪新老204路口北' AND HPHM IS NOT NULL", JList.create(new java.sql.Date(DateUtil.parse("yyyy-MM-dd HH:mm:ss", "2014-06-01 00:11:13").getTime())));
        List<List<Scan>> scans = qp.getScans();
        System.out.println(scans.size());
        System.out.println(qp.getSplits().size());

    }

}
