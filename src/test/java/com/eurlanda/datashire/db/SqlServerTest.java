
package com.eurlanda.datashire.db;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Created by Juntao.Zhang on 2014/7/9.
 */
public class SqlServerTest extends AbstractSql {
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
    public void insert() throws ClassNotFoundException, SQLException {
        Connection conn = getConn();
        PreparedStatement stmt = conn.prepareStatement(
                "INSERT INTO [AdventureWorks2008].[HumanResources].[all_type_test] ([c_bigint], [c_real], " +
                        " [c_date], [c_time], [c_xml], [c_image]) VALUES" +
                        " (?, ?, ?, ?,?, ? )"
        );
        stmt.setLong((Integer)1, Math.abs(System.nanoTime()));
        stmt.setDouble((Integer)2, 232323.23423);
        stmt.setDate((Integer)3, new java.sql.Date(System.currentTimeMillis()));
        stmt.setTime((Integer)4, new Time(System.currentTimeMillis()));
        stmt.setString((Integer)5, "sdfjsdfklsdfjlsd");
        stmt.setBytes((Integer)6, new byte[]{2, 3, 5, 23, 56, 2, -21, 3, 56, 6, 3, -12, 33, 2});
//        stmt.setString(7, UUID.randomUUID().toString());
        stmt.addBatch();
        stmt.executeBatch();
        conn.commit();
        conn.close();

    }

    @Test
    public void dropTable() throws Exception {
        Connection conn = getConn();
        conn.createStatement().executeUpdate("drop table [HumanResources].[all_type_test]");
        conn.close();
    }

    @Test
    public void createTable() throws Exception {
        String create = "CREATE TABLE [HumanResources].[all_type_test](" +
                "[c_bigint] bigint NULL ," +
                "[c_real] real NULL ," +
                "[c_timestamp] timestamp NULL ," +
                "[c_date] date NULL ," +
                "[c_time] time(7) NULL ," +
                "[c_xml] xml NULL ," +
                "[c_image] image NULL ," +
                "[c_uniqueidentifier] uniqueidentifier NULL DEFAULT (newid()) ," +
                "[c_bit] bit NULL ," +
                "[c_datetime] datetime NULL ," +
                "[c_smalldatetime] smalldatetime NULL ," +
                "[c_datetimeoffset] datetimeoffset NULL ," +
                "[c_datetime2] datetime2 NULL ," +
                "[c_nchar] nchar(1) NULL ," +
                "[c_varbinary] varbinary(2) NULL ," +
                "[c_numeric] numeric(18,3) NULL" +
                ") " +
                "ON [PRIMARY] TEXTIMAGE_ON [PRIMARY] ";
        Connection conn = getConn();
        conn.createStatement().executeUpdate(create);
        conn.close();
    }

    @Test
    public void update_type() throws ClassNotFoundException, SQLException {
//        Timestamp time = new Timestamp(System.currentTimeMillis());
//        update("date", new Timestamp(System.currentTimeMillis()));
//        update("datetime", new Timestamp(System.currentTimeMillis()));
//        update("smalldatetime", new Timestamp(System.currentTimeMillis()));
//        time.setNanos(111);
//        update("datetimeoffset", new Timestamp(120, 7, 9, 20, 1, 1, 1));
//        update("varbinary", new byte[]{21, 13});
//        update("datetime2", new Timestamp(System.currentTimeMillis()));
//        update("timestamp", new Timestamp(System.currentTimeMillis()));
//        update("time", new Timestamp(System.currentTimeMillis()));
        update("bit", true);
    }
    public void update(String _col, Object val) throws ClassNotFoundException, SQLException {
        Connection conn = getConn();
        String col = "c_" + _col;
        PreparedStatement stmt = conn.prepareStatement(
                " UPDATE TOP(1) [HumanResources].[all_type_test] SET [" + col + "] = ?"
        );
        stmt.setObject(1, val);
        stmt.executeUpdate();
        conn.commit();

        PreparedStatement pst = conn.prepareStatement("select * from HumanResources.all_type_test");
        ResultSet rs = pst.executeQuery();
        if (rs.next()) {
            try {
//                SimpleDateFormat df = DateUtil.genSDF("yyyy-mm-dd hh:mm:ss.SSSSSSS");
//                Timestamp t =   new Timestamp(df.parse(rs.getString(col)).getTime());
//                t.setNanos(1);
                Object t = rs.getObject(col);
                System.out.println(col + " | " + t + " | " + t.getClass());
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
        conn.close();
    }

    @Test
    public void timestamp() throws ClassNotFoundException, SQLException {
        Connection conn = getConn();
        String col = "c_" + "timestamp";
        PreparedStatement stmt = conn.prepareStatement(
                " UPDATE TOP(1) [HumanResources].[MyTest] SET [myValue] = ?"
        );
        stmt.setObject((Integer)1, 2);
        stmt.executeUpdate();
        conn.commit();

        PreparedStatement pst = conn.prepareStatement("select * from HumanResources.MyTest");
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {
            try {
//                SimpleDateFormat df = DateUtil.genSDF("yyyy-mm-dd hh:mm:ss.SSSSSSS");
                Object t = rs.getBytes("RV");
                System.out.println(col + " | " + t + " | " + t.getClass());
            } catch (Exception e1) {
                e1.printStackTrace();
                try {
                    Object t = rs.getObject(col);
                    System.out.println(col + " | " + t + " | " + t.getClass());
                } catch (Exception e4) {
                    e4.printStackTrace();
                }
            }


        }
        conn.close();
    }

    @Test
    public void update() throws ClassNotFoundException, SQLException {
        Connection conn = getConn();
        PreparedStatement stmt = conn.prepareStatement(
                " UPDATE TOP(1) [AdventureWorks2008].[HumanResources].[all_type_test] SET [c_bit] = ? , [c_numeric] = ?  "
        );
        stmt.setBoolean((Integer)1, true);
        stmt.setBigDecimal((Integer)2, new BigDecimal(8239243.2349782d));
        stmt.addBatch();
        stmt.executeBatch();
        conn.commit();
        conn.close();
    }

    @Test
    public void select() throws ClassNotFoundException, SQLException {
        Connection conn = getConn();

        PreparedStatement pst = conn.prepareStatement("SELECT * from Person.BusinessEntityContact where BusinessEntityID =318");
//        PreparedStatement pst = conn.prepareStatement("select * from HumanResources.all_type_test");
//        PreparedStatement pst = conn.prepareStatement("select * from HumanResources.MyTest");
        ResultSet rs = pst.executeQuery();
        if (rs.next()) {
            System.out.println(rs.getObject("ModifiedDate"));
//            System.out.println(rs.getObject("c_datetime"));
//            System.out.println(rs.getTimestamp("c_datetime"));
        }
        conn.close();
    }

    @Test
    public void updateTimestampTest() throws ClassNotFoundException, SQLException {
        Connection sqlServerConn = getSQLServerConn();
        PreparedStatement pst = sqlServerConn.prepareStatement("select * from HumanResources.all_type_test");
        ResultSet rs = pst.executeQuery();
        Timestamp timestamp = null;
        if (rs.next()) {
            timestamp = rs.getTimestamp("c_datetime");
            System.out.println(timestamp);
        }
        sqlServerConn.close();

        Connection mysqlConn = getMysqlConn();
        PreparedStatement stmt = mysqlConn.prepareStatement(
                " UPDATE all_type_test SET c_timestamp = ?"
        );
        stmt.setTimestamp(1, timestamp);
        stmt.addBatch();
        stmt.executeBatch();
        mysqlConn.close();
        mysqlConn = getMysqlConn();
        pst= mysqlConn.prepareStatement("select * from all_type_test");
        rs = pst.executeQuery();
        if (rs.next()) {
            timestamp = rs.getTimestamp("c_timestamp");
            System.out.println(timestamp);
        }
        mysqlConn.close();
    }


    private static Connection getConn() throws ClassNotFoundException, SQLException {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        return DriverManager.getConnection("jdbc:sqlserver://192.168.137.1:1433;DatabaseName=AdventureWorks2012", "sa", "squiding@eurlanda");
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
//        System.out.println("1111.2222".replace(".", "_"));
        testSql2Oracle();

    }

    public static void testSql2Sql() throws SQLException, ClassNotFoundException {
        Connection conn = getConn();
//        for(int i=0; i<1000; i++) {
//            conn.createStatement().executeUpdate("insert into dbo.s_tage(id,inttest) values(" + i + ","+ i + ")");
//        }
//        conn.createStatement().executeUpdate("insert into dbo.s_tage(id) values(10001)");
        ResultSet rs = conn.createStatement().executeQuery("select * from Production.Document");
//        Connection conn_sit = DriverManager.getConnection("jdbc:sqlserver://192.168.137.1:1433;DatabaseName=sit", "sa", "squiding@eurlanda");
        while(rs.next()) {
//            byte[] doc = rs.getBytes("document");
            byte[] docN = rs.getBytes("DocumentNode");
//            System.out.println(doc);
            System.out.println(docN);
//            PreparedStatement pst = conn_sit.prepareStatement("insert into Production_Document(Document,DocumentNode) values(?,?)");
//            pst.setObject(1,null);
//            pst.setBytes(2,docN);
//            pst.execute();
        }
//        conn_sit.close();
        conn.close();
    }

    public static void testSql2Oracle() throws SQLException, ClassNotFoundException {
        Connection conn = getConn();
        ResultSet rs = conn.createStatement().executeQuery("select document,documentnode.ToString() DocumentNodestr, DocumentNode from Production.Document");
        Class.forName("oracle.jdbc.driver.OracleDriver");
        Connection conn_oracle = DriverManager.getConnection("jdbc:oracle:thin:@192.168.137.4:1521:ADVENTURE", "system", "eurlandatest");
        while(rs.next()) {

            byte[] doc = rs.getBytes("document");
            byte[] docN = rs.getBytes("DocumentNode");
            String docNstr = rs.getString("DocumentNode");

            byte[] docN2 = rs.getBytes("DocumentNodestr");
            String docNstr2 = rs.getString("DocumentNodestr");

            System.out.println(" =====2 =====");
            System.out.println(docNstr2);
            System.out.println(printBytes(docN2));

//            System.out.println(doc);
            System.out.println("====== 1 ==========");
            System.out.println(docNstr);
            System.out.println(docN + "__" + docN.length + "___" + printBytes(docN));

            PreparedStatement pst = conn_oracle.prepareStatement("insert into ds_e_689(\"Document\",\"" +
                    "DOCUMENTNODE\") values(?,?)");
            pst.setObject(1, doc);
            pst.setBytes(2, docN);
            pst.execute();
        }
        conn_oracle.close();
        conn.close();
    }

    private static String printBytes(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for(byte b : bytes) {
            sb.append(b + "|___|");
        }
        return sb.toString();
    }
}
