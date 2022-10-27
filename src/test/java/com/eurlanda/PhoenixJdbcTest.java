package com.eurlanda;

import com.eurlanda.datashire.engine.util.DateUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by e56 on 2014/12/11.
 */
public class PhoenixJdbcTest {

    public static void main(String[] args) throws Exception{

        /**
         * 洪门蔷薇桥西高清卡口
         * 秦庄与236省道交口
         * 苍梧路与郁州路路口
         * 古龙岗
         * 马站霍家官庄北
         */
        org.xerial.snappy.Snappy an;
        String tableName = "NJ_LOAD_DATA_05";

        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:phoenix:e221,e222,e223:2181");

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            System.out.println(sdf.format(new Date()));
            long current = System.currentTimeMillis();
            ResultSet rs = conn.createStatement().executeQuery("select d1.CPHM from " + tableName + " d1 inner join " + tableName + " d2 on d1.CPHM=d2.CPHM " +
                    " INNER join " + tableName + " d3 on d1.CPHM=d3.CPHM " +
                    "where d1.KKMC='新东路与人民路' AND d1.JGSJ BETWEEN TO_DATE('2014-05-03', 'yyyy-MM-dd') " +
                    "AND TO_DATE('2014-05-04', 'yyyy-MM-dd') " +
                    "and d2.KKMC='港城平山路' AND d2.JGSJ BETWEEN  TO_DATE('2014-05-03', 'yyyy-MM-dd')  " +
                    "AND TO_DATE('2014-05-04', 'yyyy-MM-dd') " +
                    "and d3.KKMC='苍梧路与郁州路路口' AND d3.JGSJ BETWEEN  TO_DATE('2014-05-03', 'yyyy-MM-dd')  " +
                    "AND TO_DATE('2014-05-04', 'yyyy-MM-dd') " +
                    "group by d1.CPHM");

            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
            System.out.println(sdf.format(new Date()));
            System.out.println(tableName + " 耗时：" + (System.currentTimeMillis() - current) / 1000);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if(conn != null)
            conn.close();
        }
    }

    @Test
    public void testIn() throws Exception {
        String tableName = "NJ_LOAD_DATA_06";

        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:phoenix:e221,e222,e223:2181");

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            System.out.println(sdf.format(new Date()));
            long current = System.currentTimeMillis();
            String sql = "SELECT HPHM from NJ_LOAD_DATA_06 " +
                    "where (kkmc, xsfx) in (?) " +
                    "and jgsj > to_date('2014-06-01 16:13:21','yyyy-MM-dd HH:mm:ss','GMT+8') ";
            String sql2 = "SELECT HPHM from NJ_LOAD_DATA_06 " +
                    "where (kkmc, xsfx) in ((?,?), (?,?),(?,?)) " +
                    "and jgsj > to_date('2014-06-01 16:13:21','yyyy-MM-dd HH:mm:ss','GMT+8') ";
            PreparedStatement pstmt = conn.prepareStatement(sql2);
            pstmt.setString(1,"桃林陈栈西");
            pstmt.setString(2,"01");
            pstmt.setString(3,"杨集沂河大桥-卡口");
            pstmt.setString(4,"02");
            pstmt.setString(5,"环城东路大桥南");
            pstmt.setString(6,"03");
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
            System.out.println(sdf.format(new Date()));
            System.out.println(tableName + " 耗时：" + (System.currentTimeMillis() - current) / 1000);
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            if(conn != null)
                conn.close();
        }
    }
}
