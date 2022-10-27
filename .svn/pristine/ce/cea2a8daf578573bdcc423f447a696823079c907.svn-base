package com.eurlanda.datashire.db;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by Juntao.Zhang on 2014/7/9.
 */
public class PhoenixHbaseTest {
    @Test
    public void selectData() throws Exception {
        Connection conn = getConn();
        ResultSet rs = conn.createStatement().executeQuery("SELECT id,first_name,last_name,c_boolean FROM unit_test_performance");
        int i = 0;
        while (rs.next()) {
            if (i != 0 && i % 8 == 0) {
                System.out.println();
            }
            i++;
            System.out.print(rs.getObject(1) + "," + rs.getObject(2) + "," + rs.getObject(3) + "," + rs.getObject(4) + " | ");
        }
        System.out.println();
        conn.close();
    }

    @Test
    public void insertData() throws Exception {
        int count = 1;
        Connection conn = getConn();
        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO unit_test_performance (id,first_name,last_name,c_boolean) values(?,?,?,?)");
        stmt.setLong(1, Math.abs(System.nanoTime()));
        stmt.setString(2, "a" + count);
        stmt.setString(3, "b" + count);
        stmt.setBoolean(4, false);
        stmt.addBatch();
        stmt.setLong(1, Math.abs(System.nanoTime()));
        stmt.setString(2, "c" + count);
        stmt.setString(3, "b" + count);
        stmt.setBoolean(4, true);
        stmt.addBatch();
        stmt.executeBatch();
        conn.commit();
        conn.close();
    }

    @Test
    public void createTable() throws Exception {
        Connection conn = getConn();
        conn.createStatement().executeUpdate("" +
                "CREATE TABLE IF NOT EXISTS unit_test_performance (" +
                "id BIGINT NOT NULL," +
                "first_name VARCHAR NOT NULL," +
                "c_boolean boolean NOT NULL," +
                "last_name VARCHAR NOT NULL  CONSTRAINT my_pk PRIMARY KEY (id))");
        conn.close();
    }

    @Test
    public void dropTable() throws Exception {
        Connection conn = getConn();
        conn.createStatement().executeUpdate("drop table unit_test_performance");
        conn.close();
    }

    public Connection getConn() throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        Connection conn = DriverManager.getConnection("jdbc:phoenix:e231:2181");
        return conn;
    }
}
