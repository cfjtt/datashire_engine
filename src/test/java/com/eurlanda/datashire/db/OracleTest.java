package com.eurlanda.datashire.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

/**
 * Created by zhudebin on 14-7-14.
 */
public class OracleTest {

    public static void main(String[] args) throws Exception {
        Class.forName("oracle.jdbc.driver.OracleDriver");
        Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@192.168.137.4:1521:ADVENTURE", "system", "eurlandatest");
        ResultSet rs = conn.createStatement().executeQuery("select * from help");
        while(rs.next()) {
            System.out.println(rs.getObject(1));
        }
    }
}
