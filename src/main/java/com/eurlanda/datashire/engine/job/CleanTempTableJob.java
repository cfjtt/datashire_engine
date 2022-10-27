package com.eurlanda.datashire.engine.job;

import com.eurlanda.datashire.engine.util.ListUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * 清理引擎运行过程中产生的临时表（hbase）
 * 1.实时报表生成的临时表（"ds_report_tmp.t_" + taskId.replace("-", "_") + "_" + squidId）
 * 2.
 * Created by zhudebin on 14-10-23.
 */
public class CleanTempTableJob implements EngineJob {

    private static Log log = LogFactory.getLog(CleanTempTableJob.class);

    @Override
    public boolean doJob() {
        Connection conn = null;
        // 1 实时报表生成的临时表
        try {
//            conn = ConstantUtil.getSysHbaseDataSource().getConnection();
            conn.setAutoCommit(false);
//            ResultSet rs = conn.createStatement().executeQuery("select table_name,table_schem from system.catalog where table_schem='DS_REPORT_TMP' group by table_name,table_schem");
            ResultSet rs = conn.createStatement().executeQuery("select table_name,table_schem from system.catalog where table_schem!='SYSTEM' OR table_schem IS NULL group by table_name,table_schem");

            List<List<String>> list = new ArrayList<>();
            while(rs.next()) {
                String tableName = rs.getString(1);
                String schema = rs.getString(2);
                list.add(ListUtil.asList(schema, tableName));
            }
            log.info("待删除的临时表总数为：" + list.size());
            rs.close();

            Statement st = conn.createStatement();
            int idx = 0;
            for(List<String> l : list) {
                String schema = l.get(0);
                String tableName = l.get(1);
                log.info("删除临时report表 ： " + schema + "." + tableName);
                StringBuilder tableNameSb = new StringBuilder("drop table ");
                if("AA".equals(tableName)) {
                    continue;
                }
                if(schema != null) {
                    //                    if(!"NULL".equalsIgnoreCase(schema)) {
                    //
                    //                    }
                    tableNameSb.append(schema).append(".");
                }

                idx ++;
                tableNameSb.append(tableName).toString();
                System.out.println("---------- table name ------ " + tableNameSb.toString());
                st.addBatch(tableNameSb.toString());
                if(idx % 100 == 0) {
                    st.executeBatch();
                    conn.commit();
                    log.info("===========================批量删除临时表success!!===================");
                    idx = 0;
                }
            }
            if(idx > 0) {
                st.executeBatch();
                conn.commit();
                log.info("批量删除临时表success!!  全部删除完成!!");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if(conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return false;
    }

    public boolean doJob2() {
        Connection conn = null;
        // 1 实时报表生成的临时表
        try {
//            conn = ConstantUtil.getSysHbaseDataSource().getConnection();
            conn.setAutoCommit(false);
            //            ResultSet rs = conn.createStatement().executeQuery("select table_name,table_schem from system.catalog where table_schem='DS_REPORT_TMP' group by table_name,table_schem");
            ResultSet rs = conn.createStatement().executeQuery("select table_name,table_schem from system.catalog where table_schem!='SYSTEM' OR table_schem IS NULL group by table_name,table_schem");

            List<List<String>> list = new ArrayList<>();
            while(rs.next()) {
                String tableName = rs.getString(1);
                String schema = rs.getString(2);
                list.add(ListUtil.asList(schema, tableName));
            }
            log.info("待删除的临时表总数为：" + list.size());
            rs.close();

            Statement st = conn.createStatement();
            for(List<String> l : list) {
                String schema = l.get(0);
                String tableName = l.get(1);
                log.info("删除临时report表 ： " + schema + "." + tableName);
                StringBuilder tableNameSb = new StringBuilder("drop table ");
                if("AA".equals(tableName)) {
                    continue;
                }



                if(schema != null) {
                    //                    if(!"NULL".equalsIgnoreCase(schema)) {
                    //
                    //                    }
                    tableNameSb.append(schema).append(".");
                }

                tableNameSb.append(tableName).toString();
                System.out.println("---------- table name ------ " + tableNameSb.toString());

                try {
                    st.execute(tableNameSb.toString());

                    conn.commit();
                } catch (Exception e) {
                    log.error("=================删除表 异常:" + tableNameSb);
                    e.printStackTrace();
                    conn.rollback();
                    continue;
                }
            }

            log.info("批量删除临时表success!!  全部删除完成!!");

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if(conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return false;
    }

    public void listTable() {
        Connection conn = null;
        // 1 实时报表生成的临时表
        try {
//            conn = ConstantUtil.getSysHbaseDataSource().getConnection();
            conn.setAutoCommit(false);
            ResultSet rs = conn.createStatement().executeQuery("select table_name,table_schem from system.catalog where table_schem!='SYSTEM' OR table_schem IS NULL group by table_name,table_schem");

            List<List<String>> list = new ArrayList<>();
            while(rs.next()) {
                String tableName = rs.getString(1);
                String schema = rs.getString(2);
                list.add(ListUtil.asList(schema, tableName));
            }
            log.info("待删除的临时表总数为：" + list.size());
            rs.close();

            for(List<String> l : list) {
                String schema = l.get(0);
                String tableName = l.get(1);
                log.info("删除临时report表 ： " + schema + "." + tableName);

            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if(conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        new CleanTempTableJob().doJob2();
//        new CleanTempTableJob().listTable();
    }

}
