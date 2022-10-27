package com.eurlanda.datashire.engine.rpc;

import com.eurlanda.datashire.common.rpc.ICrawlerService;
import com.eurlanda.datashire.engine.service.RpcServerFactory;
import org.apache.avro.AvroRemoteException;

/**
 * Created by zhudebin on 14-5-28.
 */
public class CrawlerRPCTest {

    public static void main(String[] args) {
        testCrawlerRPC();

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


    public static void testCrawlerRPC() {
         ICrawlerService cs = RpcServerFactory.getCrawlerService();
         try {
         cs.crawlWeb("[{\"deep\":3,\"hostSet\":[\"www.csdn.net\"],\"initurls\":\"http://www.csdn.net/\",\"limitHost\":true,\"profilename\":\"csdn\",\"repositoryId\":1,\"sourceType\":\"NORMAL_WEB\",\"squidId\":99}]");
         } catch (AvroRemoteException e) {
         e.printStackTrace();
         }
        System.exit(0);
    }
}
