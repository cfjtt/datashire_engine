package com.eurlanda.datashire.hbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;

public class ClientTest3 {
	
	public static void main(String[] args) throws Exception {
		Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		Connection conn = DriverManager.getConnection("jdbc:phoenix:e201:2181");
		ResultSet rs = conn.createStatement().executeQuery("SELECT dep_name,age,last_name,first_name FROM sf_implied_5_48");
		while(rs.next()) {
            System.out.println(rs.toString() + " : " + rs.getString(1) + "," + rs.getLong(2) + "," + rs.getString(3) + "," + rs.getString(4));
		}
	}

}
