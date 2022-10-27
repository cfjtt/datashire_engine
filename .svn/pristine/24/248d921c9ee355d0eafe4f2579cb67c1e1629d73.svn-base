package com.eurlanda.datashire.hbase;

import java.sql.Connection;
import java.sql.DriverManager;

public class ClientTest {

    public static void main(String[] args) throws Exception {

        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        Connection conn = DriverManager.getConnection("jdbc:phoenix:e201:2181");

        /**
         PreparedStatement pst = conn.prepareStatement("select 1");
         ResultSet rs = pst.executeQuery();
         while(rs.next()) {
         int i = rs.getInt(0);
         System.out.println(i);
         }
         */

//		conn.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS sf_implied_5_48 (age BIGINT NOT NULL,dep_name VARCHAR NOT NULL,first_name VARCHAR NOT NULL,last_name VARCHAR NOT NULL  CONSTRAINT my_pk PRIMARY KEY (age, dep_name,first_name,last_name))");
//		conn.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS US_POPULATION2 (  state CHAR(2) NOT NULL,  city VARCHAR NOT NULL,  population BIGINT  CONSTRAINT my_pk PRIMARY KEY (state, city))");
        /**
         PreparedStatement pst = conn.prepareStatement("upsert into us_population(state, city, population) values(?,?,?)");
         for(int i=0; i<100; i++) {
         pst.setString(1, "" + i);
         pst.setString(2, "city" + i);
         pst.setInt(3, i);
         pst.execute();
         System.out.println("============================" + i + "=============");
         }
         */
        conn.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS test_1 (ID1 BIGINT NOT NULL,ID2 BIGINT NOT NULL," +
                "BUS_1 VARCHAR NOT NULL,BUS_2 VARCHAR NOT NULL, val_1 VARCHAR ,version TINYINT NOT NULL CONSTRAINT my_pk PRIMARY KEY (ID1,ID2))");

        conn.setAutoCommit(false);
//        PreparedStatement stmt = conn.prepareStatement("UPSERT INTO report_tmp.null_3(id,first_name,last_name) values(?,?,?)");
//        stmt.setObject(1,1);
//        stmt.setObject(2,"a");
//        stmt.setObject(3,"b");
//        stmt.addBatch();
//        stmt.setObject(1,2);
//        stmt.setObject(2,"c");
//        stmt.setObject(3,"b");
//        stmt.addBatch();
//        stmt.executeBatch();
        conn.commit();
        conn.close();
        //ResultSet rs = conn.createStatement().executeQuery("select 1 from us_population");
//        ResultSet rs = conn.createStatement().executeQuery("SELECT com_name,name,id,company_id FROM SF_IMPLIED_6_35");
//        while (rs.next()) {
//            System.out.println("=======================" + rs.getString(1) + "," + rs.getString(2) + "," + rs.getInt(3)+ "," + rs.getInt(4));
//        }


//        System.out.println(conn);

    }

}
