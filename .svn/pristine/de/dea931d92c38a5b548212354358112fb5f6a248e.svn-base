package com.eurlanda.datashire.engine.rpc;

import com.eurlanda.datashire.common.rpc.IReportService;
import com.eurlanda.datashire.engine.service.RpcServerFactory;

/**
 * Created by zhudebin on 14-5-28.
 */
public class ReportRPCTest {

    public static void main(String[] args) {
        testReportRPC();

        /***
        ApplicationContext context = new ClassPathXmlApplicationContext(new String[] {"conf/applicationContext.xml"});
        ConstantUtil.context = context;

//        DataSource ds = context.getBean("dataSource_inner_hbase", DataSource.class);
        DataSource ds = ConstantUtil.getSysHbaseDataSource();
        Connection conn = null;
        try {
            long startTime = System.currentTimeMillis();
            System.out.println("start time ===" + startTime);
            conn = ds.getConnection();
            ResultSet rs = conn.createStatement().executeQuery("select * from STOCK_SYMBOL");
            while(rs.next()) {
                System.out.print(rs.getObject(1));
            }
            long endTime = System.currentTimeMillis();
            System.out.println("end time ===" + (endTime-startTime)/1000);
            System.out.println("=============================");

            long startTime2 = System.currentTimeMillis();
            conn = ds.getConnection();
            ResultSet rs2 = conn.createStatement().executeQuery("select * from STOCK_SYMBOL");
            while(rs2.next()) {
                System.out.print(rs2.getObject(1));
            }
            long endTime2 = System.currentTimeMillis();
            System.out.println("end time ===" + (endTime2-startTime2)/1000);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if(conn != null && (!conn.isClosed())) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }
            */
    }


    public static void testReportRPC() {
        try {
            IReportService rs = RpcServerFactory.getReportService();
            rs.onReportSquidFinished("hello", 1, 2100);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }
}
