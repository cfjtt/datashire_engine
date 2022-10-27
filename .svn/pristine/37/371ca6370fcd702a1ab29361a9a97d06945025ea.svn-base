package com.eurlanda.datashire.engine.service;

import com.eurlanda.datashire.engine.util.ConstantUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhudebin on 14-7-24.
 */
public class CleanTableService {

    public static void main(String[] args) throws Exception {
        //
        cleanTable(ConstantUtil.getSysDataSource().getConnection());
    }

    public static void cleanTable(Connection conn) throws Exception {
        ResultSet rs = conn.createStatement().executeQuery("select distinct table_name from system.CATALOG where table_schem='DS_REPORT_TMP'");
        List<String> tblist = new ArrayList<>();
        while (rs.next()) {
            String tbname = rs.getString(1);
            tblist.add(tbname);
//            System.out.println(tbname);
        }
        rs.close();

        for(String tb : tblist) {
            conn.createStatement().executeUpdate("drop table DS_REPORT_TMP." + tb.trim());
            System.out.println("删除表[" + tb + "] 成功...");
        }
        conn.close();
    }

}
