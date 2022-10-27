package com.eurlanda.datashire.db;

import com.eurlanda.datashire.server.utils.Constants;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * Created by zhudebin on 14-7-21.
 */
public class DB2Test {

    public static void main(String[] args) throws Exception {
        testTimeStamp();
    }

    private static Connection getConn() throws ClassNotFoundException, SQLException {
        Class.forName("com.ibm.db2.jcc.DB2Driver");
        return DriverManager.getConnection("jdbc:db2://192.168.137.15:50000/sample", "test_db2", "s@eurlanda1");
    }

    public static void testTimeStamp() throws SQLException, ClassNotFoundException {
        Connection conn = getConn();
        Timestamp ts = new Timestamp(new java.util.Date().getTime());
        PreparedStatement stmt = conn.prepareStatement(
                " INSERT INTO VPHONE_EXTRACT (" + Constants.DEFAULT_EXTRACT_COLUMN_NAME + ", DEPTNAME, DEPTNUMBER, EMPLOYEENUMBER, FIRSTNAME, LASTNAME, MIDDLEINITIAL, PHONENUMBER)" +
                        " VALUES (?, 'SPIFFY COMPUTER SERVICE DIV.', 'A00', '000010', 'CHRISTINE', 'HAAS', 'I', '3978')"
        );
        stmt.setTimestamp(1, ts);
        stmt.executeUpdate();
        conn.commit();
        conn.close();
    }

    @Test
    public void testInsert() throws Exception {
        Connection con = getConn();
        int i = con.createStatement().executeUpdate("insert into test(id,age)values(1,1)");
        System.out.println(i);


    }

    @Test
    public void testQuery() throws Exception {
        Connection con = getConn();
        ResultSet rs = con.createStatement().executeQuery("select * from test");
        while(rs.next()) {
            System.out.println(rs.getString(1));
        }
    }
}
