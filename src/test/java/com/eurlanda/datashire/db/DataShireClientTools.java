package com.eurlanda.datashire.db;

import com.eurlanda.datashire.engine.util.ConstantUtil;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhudebin on 14-7-16.
 */
public class DataShireClientTools {

    public static void main(String[] args) {
        try {
            changeDistinationDB(3, 17, 546, 1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 修改 落地目标，设置清空数据
     */
    public static void changeDistinationDB(int repId, int squidFlowId, int destinationSquidId, int truncateFlag) throws SQLException {
        DataSource repds = ConstantUtil.getSysDataSource();
        Connection conn = repds.getConnection();
        System.out.println(squidFlowId);
        ResultSet rs = conn.createStatement().executeQuery("select id from  ds_squid  where squid_flow_id=" + squidFlowId);
        List<Integer> idList = new ArrayList<>();
        while(rs.next()) {
            System.out.println("====1=====");
            System.out.println(rs.getObject(1));
            idList.add(rs.getInt(1));
        }

//        conn.setAutoCommit(false);
        String sql = "update ds_squid set destination_squid_id=?,table_name=?, truncate_existing_data_flag=?\n" +
                "where id =?";
        String sql2 = "update ds_squid set table_name=? where id =?";
        PreparedStatement pst = conn.prepareStatement(sql);
        PreparedStatement pst2 = conn.prepareStatement(sql2);
        for(int id : idList) {
            System.out.println(id);
            pst.setInt(1, destinationSquidId);
            pst.setString(2, "ds_e_" + id);
            pst.setInt(3, truncateFlag);
            pst.setInt(4, id);
            pst.executeUpdate();
            pst2.setString(1, "ds_e_" + id);
            pst2.setInt(2, id);
            pst2.executeUpdate();
        }
//        pst.executeBatch();
//        conn.commit();
        if(conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

}
