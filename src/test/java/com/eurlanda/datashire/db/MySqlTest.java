
package com.eurlanda.datashire.db;

import com.eurlanda.datashire.engine.util.DateUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DateFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Created by Juntao.Zhang on 2014/7/9.
 */
public class MySqlTest {
    @BeforeClass
    public static void init() throws Exception {
//        dropTable();
//        createTable();
//        insert();
    }

    @AfterClass
    public static void terminate() throws Exception {
//        dropTable();
    }

    @Test
    public void test1()  {
        double d1 =  2222222222222222222222222222444222222222.222222222222222222222;
        String dd  = "2222222222222222222222222222444222222222.222222222222222222222";
        String dd2 = "2222222222222222222222222222444222222223.222222222222222222222";
        Double ddd = new Double(dd);
        String str = Double.toString(d1);
        System.out.println(ddd - d1);

        System.out.println(Double.toString(d1));
        BigDecimal db = new BigDecimal(dd);
        BigDecimal db2 = new BigDecimal(dd2);
        BigDecimal db3 = BigDecimal.valueOf(ddd);
        System.out.println(db3.subtract(db2));
        System.out.println(db.subtract(db2));
        System.out.println(db.subtract(db3));
        db = new BigDecimal(d1);
        System.out.println(db.doubleValue());
    }

    @Test
    public void test2()  {
        String str2 = "123456789012345678901.0";
        double d = 123456789012345678901.0;
        String str = Double.toString(d);
        System.out.println(Double.toString(d));
        double re = new Double(Double.toString(d)) - d;
        BigDecimal bd = new BigDecimal(str).subtract(new BigDecimal(str2));
        System.out.println(bd);
        System.out.println(re);
        BigDecimal bd3 = new BigDecimal(d);
        bd3.setScale(100);
        System.out.println(bd3);
        System.out.println(Long.MAX_VALUE);

        double dd = 12345678901234568000.0;
        System.out.println(String.valueOf(d));

        String t = "01/01/12";
        try {
            System.out.println(DateUtil.parse("dd/MM/yy", t));
            System.out.println(DateUtil.format("dd/MMM/yy", Locale.FRANCE, new Date()));
            System.out.println(DateUtil.format("dd/MMM/yy", new Date()));

            System.out.println("1" + DateUtil.parse("dd/MMM/yy", "25/十二月/14"));
            System.out.println("2" + DateUtil.parse("dd/MMM/yy", "25/Dec/14"));
            System.out.println("3" + DateUtil.parse("dd/MMM/yy", "25/十二月/14"));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test3()  {

        String str1 = "2014-11-11T20:20:20";
        DateFormatSymbols dfs = DateFormatSymbols.getInstance(Locale.ENGLISH);
        dfs.setLocalPatternChars("T");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        try {
            System.out.println(sdf.parse(str1));
            System.out.println(sdf.format(sdf.parse(str1)));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        System.out.println(1/0.0);

        for(int i=0; i<=180; i++) {
            double str = Math.sin(Math.toRadians(i));
            System.out.println(i + " : " + str);
            System.out.println(i + " : " + Math.toDegrees(Math.asin(str)));
        }
    }

    @Test
    public void insert() throws ClassNotFoundException, SQLException {
        Connection conn = getConn();
        conn.setAutoCommit(false);
        PreparedStatement stmt = conn.prepareStatement(
                "INSERT INTO `unit_test`.`all_type_test` (`c_bigint`) VALUES (?)"
        );
        stmt.setLong(1, Math.abs(System.nanoTime()));
        stmt.addBatch();
        stmt.executeBatch();
        conn.commit();
        conn.close();
    }
    @Test
    public void createTable() throws Exception {

        String create = " CREATE TABLE `all_type_test` " +
                "(" +
                "`c_bigint` bigint(20) DEFAULT NULL," +
                "`c_timestamp` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP," +
                "`c_datetime` datetime DEFAULT NULL," +
                "`c_int` int(11) DEFAULT NULL," +
                "` c_smallint` smallint(6) DEFAULT NULL," +
                "`c_tinyint` tinyint(4) DEFAULT NULL," +
                "`c_bit` bit(1) DEFAULT NULL," +
                "`c_decimal` decimal(60,10) DEFAULT NULL," +
                "`c_enum` enum('2','1') DEFAULT NULL," +
                "`c_set` set('a','d','v') DEFAULT NULL," +
                "`c_text` text DEFAULT NULL" +
                ")" +
                " ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin";
        Connection conn = getConn();
        conn.createStatement().executeUpdate(create);
        conn.close();
    }

    @Test
    public void update_type() throws ClassNotFoundException, SQLException {
//        update("timestamp", new Timestamp(System.currentTimeMillis()));
//        update("bit", true);
//        update("decimal", new BigDecimal(129783912.32423d));
//        update("tinyint", 41);
//        update("text", "sdfjlskdflsd");
//        update("enum", "1");
        update("set", "a");
    }

    public void update(String _col, Object val) throws ClassNotFoundException, SQLException {
        Connection conn = getConn();
        String col = "c_" + _col;
//        conn.setAutoCommit(false);
//        PreparedStatement stmt = conn.prepareStatement(
//                " update all_type_test set " + col + " = ?"
//        );
//        stmt.setObject(1, val);
//        stmt.executeUpdate();
//        conn.commit();

        PreparedStatement pst = conn.prepareStatement("select * from all_type_test");
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            try {
//                SimpleDateFormat df = DateUtil.genSDF("yyyy-mm-dd hh:mm:ss.SSSSSSS");
                Object t = rs.getObject(col);
                System.out.println(col + " | " + t + " | " + t.getClass());
            } catch (Exception e1) {
                e1.printStackTrace();
            }


        }
        conn.close();
    }
    @Test
    public void select() throws ClassNotFoundException, SQLException {
        Connection conn = getConn();
        PreparedStatement pst = conn.prepareStatement("select * from all_type_test");
        ResultSet rs = pst.executeQuery();
        if (rs.next()) {
            System.out.println(rs.getTimestamp("c_datetime"));
            System.out.println(rs.getTimestamp("c_timestamp"));
        }
        conn.close();
    }
    @Test
    public void dropTable() throws Exception {
        Connection conn = getConn();
        conn.createStatement().executeUpdate("   drop  table if EXISTS all_type_test");
        conn.close();
    }

    private static Connection getConn() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
//        return DriverManager.getConnection("jdbc:mysql://192.168.137.2:3306/unit_test", "root", "root");
        return DriverManager.getConnection("jdbc:mysql://192.168.137.17:3306/test?autoReconnect=true&failOverReadOnly=false&maxReconnects=10", "root", "111111");
    }

    @Test
    public void testYear() throws Exception {
        Connection con = getConn();

        ResultSet rs = con.createStatement().executeQuery("select name4 from mothertype");
        rs.next();
        ResultSetMetaData rmd = rs.getMetaData();
        int count = rmd.getColumnCount();
        for(int i=0; i<count; i++) {

            rmd.getColumnType(i);
        }
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Connection conn = getConn();
        ResultSet rs = conn.createStatement().executeQuery("select 1");
        rs.next();
        System.out.println(rs.getInt(1));
        conn.close();
    }
}
