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
public class CleanTableTools implements EngineJob {

    private static Log log = LogFactory.getLog(CleanTableTools.class);

    @Override
    public boolean doJob() {
        Connection conn = null;
        // 1 实时报表生成的临时表
        try {
//            conn = ConstantUtil.getSysHbaseDataSource().getConnection();
            conn.setAutoCommit(false);
            ResultSet rs = conn.createStatement().executeQuery("select table_name,table_schem from system.catalog where table_schem IS null group by table_name,table_schem");

            List<List<String>> list = new ArrayList<>();
            while(rs.next()) {
                String tableName = rs.getString(1);
                String schema = rs.getString(2);
                list.add(ListUtil.asList(schema, tableName));
            }
            log.info("待删除的临时表总数为：" + list.size());
            rs.close();

            if(1==0) {
                return false;
            }
            Statement st = conn.createStatement();
            int idx = 0;
            for(List<String> l : list) {
                idx ++;
                String schema = l.get(0);
                String tableName = l.get(1);
                String tableName2 = l.get(1).toUpperCase();
                if(tableName2.indexOf("TEST")>-1 || tableName2.indexOf("S_TABLE")>-1) {
                    log.info("删除临时report表 ： " + schema + "." + tableName);
                    st.addBatch(new StringBuilder("drop table \"")
//                        .append(schema).append(".")
                            .append(tableName).append("\"").toString());
                } else {
                    log.info("表名：" + tableName);
                    continue;
                }
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

    public static void main(String[] args) {
        new CleanTableTools().doJob();
    }

}
