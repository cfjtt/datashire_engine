package com.eurlanda;

import com.eurlanda.datashire.engine.util.DateUtil;
import com.eurlanda.datashire.engine.util.UUIDUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by e56 on 2014/12/11.
 */
public class PhoenixTest2 {

    public static void main(String[] args) throws Exception{

        /**
         * 洪门蔷薇桥西高清卡口
         * 秦庄与236省道交口
         * 苍梧路与郁州路路口
         * 古龙岗
         * 马站霍家官庄北
         */

        String tableName = "NJ_LOAD_DATA_05";

        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:phoenix:e221,e222,e223:2181");

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            System.out.println(sdf.format(new Date()));
            long current = System.currentTimeMillis();
            String sql = "select d1.CPHM from NJ_LOAD_DATA_05 d1 inner join  " +
                    "(select CPHM FROM " +
                    "NJ_LOAD_DATA_05 WHERE KKMC='港城平山路' AND JGSJ BETWEEN  TO_DATE('2014-05-03', 'yyyy-MM-dd') AND TO_DATE('2014-05-06', 'yyyy-MM-dd')  GROUP BY CPHM) d2 on d1.CPHM=d2.CPHM " +
                    "INNER JOIN  " +
                    "(select CPHM FROM " +
                    "NJ_LOAD_DATA_05 WHERE KKMC='古龙岗' AND JGSJ BETWEEN  TO_DATE('2014-05-03', 'yyyy-MM-dd') AND TO_DATE('2014-05-06', 'yyyy-MM-dd')  GROUP BY CPHM) d3 on d1.CPHM=d3.CPHM " +
                    "INNER JOIN  " +
                    "(select CPHM FROM " +
                    "NJ_LOAD_DATA_05 WHERE KKMC='秦庄与236省道交口' AND JGSJ BETWEEN  TO_DATE('2014-05-03', 'yyyy-MM-dd') AND TO_DATE('2014-05-06', 'yyyy-MM-dd')  GROUP BY CPHM) d4 on d1.CPHM=d4.CPHM " +
                    "INNER JOIN  " +
                    "(select CPHM FROM " +
                    "NJ_LOAD_DATA_05 WHERE KKMC='马站霍家官庄北' AND JGSJ BETWEEN  TO_DATE('2014-05-03', 'yyyy-MM-dd') AND TO_DATE('2014-05-06', 'yyyy-MM-dd')  GROUP BY CPHM) d5 on d1.CPHM=d5.CPHM " +
                    "where d1.KKMC='新东路与人民路' AND d1.JGSJ BETWEEN TO_DATE('2014-05-03', 'yyyy-MM-dd') AND TO_DATE('2014-05-06', 'yyyy-MM-dd') " +
                    "group by d1.CPHM";
            ResultSet rs = conn.createStatement().executeQuery(sql);

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
    public void test2() {
        System.out.println(UUIDUtil.genUUID().length());
    }
}
