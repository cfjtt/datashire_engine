package com.eurlanda.datashire.db;

import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by zhudebin on 16/8/8.
 */
public class ImpalaTest {

    @Test
    public void testBigDecimal1() throws SQLException, ClassNotFoundException {

        BigDecimal bd = new BigDecimal(2.11122343434);


        Connection con = getConn();
        PreparedStatement pst = con.prepareStatement("insert into all_type31(cd) values (?)");
        pst.setBigDecimal(1, bd);
        pst.executeUpdate();
        con.close();
    }

    @Test
    public void testBigDecimal2() throws SQLException, ClassNotFoundException {

        BigDecimal bd = new BigDecimal(3.11122343434);

        System.out.println(bd);

        Connection con = getConn();
        PreparedStatement pst = con.prepareStatement("insert into all_type31(cd) values (?)");
        pst.setDouble(1, bd.doubleValue());
        pst.executeUpdate();
        con.close();
    }

    @Test
    public void testBigDecimal3() throws SQLException, ClassNotFoundException {

        BigDecimal bd = new BigDecimal(4.11122343434243234314123412341234);

        System.out.println(bd);

        Connection con = getConn();
        PreparedStatement pst = con.prepareStatement("insert into all_type31(cd) values (?)");
        pst.setFloat(1, bd.floatValue());
        pst.executeUpdate();
        con.close();
    }

    @Test
    public void testFloat1() throws SQLException, ClassNotFoundException {

        float f = 1.22222223333344444f;

        Connection con = getConn();
        PreparedStatement pst = con.prepareStatement("insert into all_type31(cd) values (?)");
        pst.setFloat(1, f);
        pst.executeUpdate();
        con.close();
    }

    private static Connection getConn() throws ClassNotFoundException, SQLException {
        Class.forName("com.cloudera.impala.jdbc41.Driver");
        //        return DriverManager.getConnection("jdbc:mysql://192.168.137.2:3306/unit_test", "root", "root");
        return DriverManager.getConnection("jdbc:impala://192.168.137.103:21050/default;auth=noSasl", "root", "111111");
    }
}
