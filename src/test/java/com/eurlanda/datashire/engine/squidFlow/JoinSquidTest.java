package com.eurlanda.datashire.engine.squidFlow;

import com.eurlanda.datashire.engine.entity.TColumn;
import com.eurlanda.datashire.engine.entity.TDataSource;
import com.eurlanda.datashire.engine.entity.TDataType;
import com.eurlanda.datashire.engine.entity.TDatabaseSquid;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.JoinType;

/**
 * 各种 join squid
 * Created by Juntao.Zhang on 2014/4/18.
 */
public class JoinSquidTest extends AbstractSquidTest {
    public static void main(String[] args) throws Exception {
        //    CustomJavaSparkContext customJavaSparkContext = new CustomJavaSparkContext(getSparkMasterUrl(), "JoinSquidTest", getSparkHomeDir(), getSparkJarLocation());
//    createSquidFlow(JoinType.LeftOuterJoin, "left_outer_join_person_company").run(customJavaSparkContext);
//    createSquidFlow(JoinType.RightOuterJoin, "right_outer_join_person_company").run(customJavaSparkContext);
//    createSquidFlow(JoinType.FullJoin, "full_join_person_company").run(customJavaSparkContext);
//    createSquidFlow(JoinType.CrossJoin, "cross_join_person_company").run(customJavaSparkContext);

        launch(JoinType.LeftOuterJoin, "left_outer_join_sale_order_product");
    }

    private static void launch(JoinType type, String tableName) throws Exception {
//        IEngineMockService obj = RpcServerFactory.getEngineMockService();
//        TSquidFlow joinFlow = createSquidFlow(type, tableName);
//        obj.callEngineJobMock(joinFlow);
////        System.out.println(obj.take(joinFlow.getTaskId(), 1111, 3));
//        System.out.println(obj.take(joinFlow.getTaskId(), 1111, 100));
//    System.out.println(obj.takeSample(joinFlow.getTaskId(), 1111, true, 10, new Random().nextInt()));
//    System.out.println(obj.takeSample(joinFlow.getTaskId(), 1111, false, 8, 2));
//        Thread.sleep(1000 * 5);
//        System.out.println(obj.resumeEngine(joinFlow.getTaskId(), 1111));
    }

    /*
    private static TSquidFlow createSquidFlow(JoinType joinType, String tableName) {
        TSquidFlow squidFlow = createSquidFlow(30020, 30021);

        TDatabaseSquid right = createProductDatabaseSquid();

        TDatabaseSquid left = createSalesOrderDetailDatabaseSquid();

        TJoinSquid1 joinSquid = createJoinSquid(left, right, joinType);

        TDataFallSquid dataFallSquid = createDataFallSquid(joinSquid, tableName);

        squidFlow.addSquid(right);
        squidFlow.addSquid(left);
        squidFlow.addSquid(joinSquid);
        squidFlow.addSquid(dataFallSquid);
        return squidFlow;
    } */

    /*
    private static TDataFallSquid createDataFallSquid(TJoinSquid1 joinSquid, String tableName) {
        TDataSource dataSource = new TDataSource("192.168.137.2", 3306,
                "squidflowtest", "root", "root", tableName, DataBaseType.MYSQL);

        TDataFallSquid dataFallSquid = new TDataFallSquid();
        dataFallSquid.setId("4");
        dataFallSquid.setSquidId(4);
        Set<TColumn> columnSet = new HashSet<>();
        columnSet.add(new TColumn("SalesOrderID", 11, TDataType.LONG, true));
        columnSet.add(new TColumn("OrderQty", 12, TDataType.LONG, true));
        columnSet.add(new TColumn("ProductName", 13, TDataType.STRING, true));

        dataFallSquid.setColumnSet(columnSet);
        dataFallSquid.setDataSource(dataSource);
        dataFallSquid.setPreviousSquid(joinSquid);
        return dataFallSquid;
    } */

    /*
    private static TJoinSquid1 createJoinSquid(TDatabaseSquid left, TDatabaseSquid right, JoinType joinType) {
        TJoinSquid1 joinSquid = new TJoinSquid1();
        joinSquid.setId("3");
        joinSquid.setSquidId(3);
        joinSquid.setJoinType(joinType);
        joinSquid.setLeftSquid(left);
        joinSquid.setRightSquid(right);

        List<TJoinCondition> jcList = new ArrayList<>();
        TJoinCondition jc1 = new TJoinCondition();
        jc1.setLeftKeyId(2);
        jc1.setRightKeyId(5);
        jcList.add(jc1);
        joinSquid.setJoinConditionList(jcList);
        joinSquid.setInKeyList(keyList(1, 3, 6));
        joinSquid.setOutKeyList(keyList(10, 11, 12));
        return joinSquid;
    } */

    private static TDatabaseSquid createProductDatabaseSquid() {
        TDataSource dataSource = new TDataSource("192.168.137.153", 3816,
                "AdventureWorks2008R2", "1111", "sa", "production.Product", DataBaseType.SQLSERVER);
        // product database 2
        TDatabaseSquid databaseSquid = new TDatabaseSquid();
        databaseSquid.setId("2");
        databaseSquid.setSquidId(2);
        databaseSquid.setDataSource(dataSource);
        databaseSquid.putColumn(new TColumn("ProductId", 5, TDataType.LONG));
        databaseSquid.putColumn(new TColumn("Name", 6, TDataType.STRING));
        return databaseSquid;
    }

    private static TDatabaseSquid createSalesOrderDetailDatabaseSquid() {
        TDataSource dataSource = new TDataSource("192.168.137.153", 3816,
                "AdventureWorks2008R2", "1111", "sa", "Sales.SalesOrderDetail", DataBaseType.SQLSERVER);
        // salesOrderDetail database 1
        TDatabaseSquid databaseSquid = new TDatabaseSquid();
        databaseSquid.setDataSource(dataSource);
        databaseSquid.putColumn(new TColumn("SalesOrderID", 1, TDataType.LONG));
        databaseSquid.putColumn(new TColumn("ProductId", 2, TDataType.LONG));
        databaseSquid.putColumn(new TColumn("OrderQty", 3, TDataType.LONG));
        databaseSquid.setId("1");
        databaseSquid.setSquidId(1);
        return databaseSquid;
    }


}
