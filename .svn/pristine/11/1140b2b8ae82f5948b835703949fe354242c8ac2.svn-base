package com.eurlanda.datashire.engine.squidFlow;

import com.eurlanda.datashire.engine.entity.TColumn;
import com.eurlanda.datashire.engine.entity.TDataFallSquid;
import com.eurlanda.datashire.engine.entity.TDataSource;
import com.eurlanda.datashire.engine.entity.TDataType;
import com.eurlanda.datashire.engine.entity.TDatabaseSquid;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.JoinType;

import java.util.HashSet;
import java.util.Set;

/**
 * 各种 join squid
 * Created by Juntao.Zhang on 2014/4/18.
 */
public class JoinSquidWithFilterTest extends AbstractSquidTest {
//    static String IP ="192.168.137.104";
    static String IP ="192.168.137.2";
    public static void main(String[] args) throws Exception {
//        launch(JoinType.InnerJoin, "inner_join_person_company_with_filter");
        launch(JoinType.InnerJoin, "inner_join_person_company_with_filter2");
    }

    private static void launch(JoinType type, String tableName) throws Exception {
//        TSquidFlow flow = createSquidFlow(type, tableName);
//        flow.run(new CustomJavaSparkContext(getSparkMasterUrl(), "test", getSparkHomeDir(), getSparkJarLocation()));
    }

    /*
    public static TSquidFlow createSquidFlow(JoinType joinType, String tableName) {
        TSquidFlow squidFlow = createSquidFlow(30010, 30011);

        TDatabaseSquid personSquid = createPersonDatabaseSquid();

        TDatabaseSquid companySquid = createCompanyDatabaseSquid();


        TJoinSquid1 joinSquid = createJoinSquid(personSquid, companySquid, joinType);

        TDataFallSquid dataFallSquid = createDataFallSquid(joinSquid, tableName);


        squidFlow.addSquid(personSquid);
        squidFlow.addSquid(companySquid);
        squidFlow.addSquid(joinSquid);
        squidFlow.addSquid(dataFallSquid);
        return squidFlow;
    }*/

    private static TDataFallSquid createDataFallSquid(TSquid preSquid, String tableName) {
        TDataSource dataSource = new TDataSource(IP, 3306,
                "squidflowtest", "root", "root", tableName, DataBaseType.MYSQL);

        TDataFallSquid dataFallSquid = new TDataFallSquid();
        dataFallSquid.setId("4");
        dataFallSquid.setSquidId(4);

        Set<TColumn> columnSet = new HashSet<>();
        columnSet.add(new TColumn("person_id", 6, TDataType.LONG, true));
        columnSet.add(new TColumn("person_name", 7, TDataType.STRING, true));
        columnSet.add(new TColumn("company_name", 8, TDataType.STRING, true));

        dataFallSquid.setColumnSet(columnSet);
        dataFallSquid.setDataSource(dataSource);
        dataFallSquid.setPreviousSquid(preSquid);
        return dataFallSquid;
    }

    /*
    private static TJoinSquid1 createJoinSquid(TDatabaseSquid personSquid, TDatabaseSquid companySquid, JoinType joinType) {
        TJoinSquid1 joinSquid = new TJoinSquid1();
        joinSquid.setId("3");
        joinSquid.setSquidId(3);
        joinSquid.setJoinType(joinType);
        joinSquid.setLeftSquid(personSquid);
        joinSquid.setRightSquid(companySquid);

        List<TJoinCondition> jcList = new ArrayList<>();
        TJoinCondition jc1 = new TJoinCondition();
        jc1.setLeftKeyId(3);
        jc1.setRightKeyId(4);
        jcList.add(jc1);
        joinSquid.setJoinConditionList(jcList);
        joinSquid.setInKeyList(keyList(1, 2, 5));
        joinSquid.setOutKeyList(keyList(6, 7, 8));
        return joinSquid;
    } */

    private static TDatabaseSquid createCompanyDatabaseSquid() {
        TDataSource dataSource = new TDataSource(IP, 3306,
                "squidflowtest", "root", "root", "company", DataBaseType.MYSQL);
        dataSource.setFilter("name like 'A%'");

        TDatabaseSquid companySquid = new TDatabaseSquid();
        companySquid.setId("2");
        companySquid.setSquidId(2);
        companySquid.setDataSource(dataSource);
        companySquid.putColumn(new TColumn("id", 4, TDataType.LONG));
        companySquid.putColumn(new TColumn("name", 5, TDataType.STRING));
        return companySquid;
    }

    private static TDatabaseSquid createPersonDatabaseSquid() {
        TDataSource dataSource = new TDataSource(IP, 3306,
                "squidflowtest", "root", "root", "person", DataBaseType.MYSQL);
        dataSource.setFilter("name like '%A%'");

        TDatabaseSquid personSquid = new TDatabaseSquid();
        personSquid.setId("1");
        personSquid.setSquidId(1);
        personSquid.setDataSource(dataSource);
        personSquid.putColumn(new TColumn("id", 1, TDataType.LONG));
        personSquid.putColumn(new TColumn("name", 2, TDataType.STRING));
        personSquid.putColumn(new TColumn("company_id", 3, TDataType.LONG));

        return personSquid;
    }


}
