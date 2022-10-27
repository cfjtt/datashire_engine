package com.eurlanda.datashire.engine.util;

import com.eurlanda.datashire.adapter.db.HbaseAdapter;
import com.eurlanda.datashire.entity.Column;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by zhudebin on 14-7-14.
 */
public class DBUtils {

    /**
     * 创建hbase 落地表,
     * report的实时报表，需要在hbase中隐式的创建落地表
     * @param tableName 表名
     * @param columns Column集合
     */
    public static void createHbaseTable(String tableName, List<Column> columns) {
        Connection conn = null;
        try {
            conn = ConstantUtil.getSysDataSource().getConnection();
            conn.createStatement().executeUpdate(HbaseAdapter.getTableCreateSqlByColumn(tableName, columns));
        } catch (SQLException e) {
            throw new RuntimeException("创建hbase表失败，" + tableName, e);
        } finally {
            try {
                if(conn!=null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
