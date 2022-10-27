package com.eurlanda.datashire.hbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Locale;

public class ClientTest2 {
	
	public static void main(String[] args) throws Exception {
		
//		testT_USER();
//		testT_DEP();
//    select();
//		testJoin();
//		testJoinGroupHaving();
        select2();
	}
	
	public static void test1() throws Exception {
		Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		Connection conn = DriverManager.getConnection("jdbc:phoenix:e201:2181");

		conn.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS STOCK_SYMBOL (SYMBOL VARCHAR NOT NULL PRIMARY KEY, COMPANY VARCHAR)");
		long start = System.currentTimeMillis();
		for(int i=0; i<20000; i++) {
			conn.createStatement().executeUpdate("UPSERT INTO STOCK_SYMBOL VALUES ('CRM" + i + "','SalesForce.com')");
		}
		long end = System.currentTimeMillis();
		//conn.createStatement().executeUpdate("delete from STOCK_SYMBOL");
		conn.commit();
		ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM STOCK_SYMBOL order by SYMBOL");
		
		while(rs.next()) {
			System.out.println("---------------1---------------" + rs.getString(1) + "," + rs.getString(2));
		}
		System.out.println("===================================" + (end-start));
		conn.close();
	}
	
	/**
	 * t_user
	 * @throws Exception
	 */
	public static void testT_USER() throws Exception {
		Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		Connection conn = DriverManager.getConnection("jdbc:phoenix:e201:2181");
		
		conn.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS TEST.T_USER (ID INTEGER NOT NULL PRIMARY KEY desc, USER_NAME VARCHAR, DEP_ID INTEGER)");
		long start = System.currentTimeMillis();
		for(int i=11; i<10050; i++) {
			conn.createStatement().executeUpdate("UPSERT INTO TEST.T_USER VALUES (" + i + ",'NAME_" + i + "', " + i%2100 + ")");
		}
		long end = System.currentTimeMillis();
		//conn.createStatement().executeUpdate("delete from STOCK_SYMBOL");
		conn.commit();
		ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TEST.T_USER order by ID");
		
		while(rs.next()) {
			System.out.println("---------------1---------------" + rs.getString(1) + "," + rs.getString(2));
		}
		System.out.println("===================================" + (end-start));
		conn.close();
	}
	
	/**
	 * t_dep
	 * @throws Exception
	 */
	public static void testT_DEP() throws Exception {
		Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		Connection conn = DriverManager.getConnection("jdbc:phoenix:e201:2181");
		
		conn.createStatement().executeUpdate("CREATE TABLE IF NOT EXISTS TEST.T_DEP (ID INTEGER NOT NULL PRIMARY KEY, DEP_NAME VARCHAR)");
		long start = System.currentTimeMillis();
		for(int i=4; i<2100; i++) {
			conn.createStatement().executeUpdate("UPSERT INTO TEST.T_DEP VALUES (" + i + ",'DEP_" + i + "')");
		}
		long end = System.currentTimeMillis();
		//conn.createStatement().executeUpdate("delete from STOCK_SYMBOL");
		conn.commit();
		ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TEST.T_DEP order by ID");
		
		while(rs.next()) {
			System.out.println("---------------1---------------" + rs.getString(1) + "," + rs.getString(2));
		}
		System.out.println("===================================" + (end-start));
		conn.close();
	}

	/**
	 * t_user join t_dep on id
	 * @throws Exception
	 */
	public static void testJoin() throws Exception {
		Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		Connection conn = DriverManager.getConnection("jdbc:phoenix:e201:2181");
		
		long start = System.currentTimeMillis();
		ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TEST.T_USER AS USER  left join TEST.T_DEP AS DEP ON USER.DEP_ID = DEP.ID  order by USER.ID");
		while(rs.next()) {
			System.out.println("---------------1---------------" + rs.getString(1) + "," + rs.getString(2) + "," + rs.getString(3) + "," + rs.getString(4) + "," + rs.getString(5));
		}
		long end = System.currentTimeMillis();
		//conn.createStatement().executeUpdate("delete from STOCK_SYMBOL");
		
		
		System.out.println("===================================" + (end-start));
		conn.close();
	}
	
	/**
	 * t_user join t_dep on id
	 * @throws Exception
	 */
	public static void testJoinGroupHaving() throws Exception {
		Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		Connection conn = DriverManager.getConnection("jdbc:phoenix:e201:2181");
		
		long start = System.currentTimeMillis();
		ResultSet rs = conn.createStatement().executeQuery("SELECT USER.DEP_ID,MAX(DEP.DEP_NAME), COUNT(*) FROM TEST.T_USER AS USER  left join TEST.T_DEP AS DEP ON USER.DEP_ID = DEP.ID GROUP BY USER.DEP_ID HAVING USER.DEP_ID<20 ORDER BY USER.DEP_ID LIMIT 10");
		while(rs.next()) {
			System.out.println("---------------1---------------" + rs.getString(1) + "," + rs.getString(2) + "," + rs.getString(3));
		}
		long end = System.currentTimeMillis();
		//conn.createStatement().executeUpdate("delete from STOCK_SYMBOL");
		
		
		System.out.println("===================================" + (end-start));
		conn.close();
	}

  public static void select() throws Exception {
		Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		Connection conn = DriverManager.getConnection("jdbc:phoenix:e201:2181");

		long start = System.currentTimeMillis();
//		ResultSet rs = conn.createStatement().executeQuery("SELECT count(*) FROM TEST.T_USER");
		ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM TEST.T_USER LIMIT 10");
		while(rs.next()) {
//			System.out.println("---------------1---------------" + rs.getString(1));
			System.out.println("---------------1---------------" + rs.getString("ID") + "," + rs.getString("USER_NAME")+ "," + rs.getString("DEP_ID"));
		}
		long end = System.currentTimeMillis();
		//conn.createStatement().executeUpdate("delete from STOCK_SYMBOL");


		System.out.println("===================================" + (end-start));
		conn.close();
	}

    public static void select2() throws Exception {
//        SimpleDateFormat sdf = DateUtil.genSDF("yyyy/MM/dd hh:mm:sss");
//        Locale[] locales = sdf.getNumberFormat().getAvailableLocales();
//        for(Locale l : locales) {
//            System.out.println(l.getDisplayLanguage());
//        }

        Locale.setDefault(Locale.ENGLISH);
        System.out.println(Locale.getDefault().getDisplayLanguage());
    }
}
