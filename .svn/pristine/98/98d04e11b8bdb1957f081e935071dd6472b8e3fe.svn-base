package com.eurlanda.datashire.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by Juntao.Zhang on 2014/7/16.
 */
public class AbstractSql {
    protected static Connection getMysqlConn() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        return DriverManager.getConnection("jdbc:mysql://192.168.137.2:3306/unit_test", "root", "root");
    }
    protected static Connection getSQLServerConn() throws ClassNotFoundException, SQLException {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        return DriverManager.getConnection("jdbc:sqlserver://192.168.137.1:1433;DatabaseName=AdventureWorks2008", "sa", "squiding@eurlanda");
    }
}
