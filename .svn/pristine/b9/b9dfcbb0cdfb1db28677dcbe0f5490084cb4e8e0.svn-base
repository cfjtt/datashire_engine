package com.eurlanda.datashire.engine.squidFlow;

import com.eurlanda.datashire.engine.entity.TColumn;
import com.eurlanda.datashire.engine.entity.TDataFallSquid;
import com.eurlanda.datashire.engine.entity.TDataSource;
import com.eurlanda.datashire.engine.entity.TDataType;
import com.eurlanda.datashire.engine.entity.TDatabaseSquid;
import com.eurlanda.datashire.engine.entity.TSquidFlow;
import com.eurlanda.datashire.engine.entity.TUnionSquid;
import com.eurlanda.datashire.engine.entity.TUnionType;
import com.eurlanda.datashire.enumeration.DataBaseType;

import java.util.HashSet;
import java.util.Set;

import static com.eurlanda.datashire.engine.util.ListUtil.asList;

/**
 * union quid 测试类
 * Created by Juntao.Zhang on 2014/4/18.
 */
public class UnionSquidTest extends AbstractSquidTest {
    public static void main(String[] args) throws Exception {
//        CustomJavaSparkContext cjsc = new CustomJavaSparkContext(getSparkMasterUrl(), "JoinSquidTest", getSparkHomeDir(), getSparkJarLocation());
//        createSquidFlow(TUnionType.UNION_ALL, "union_all_person_person").run(cjsc);
//        createSquidFlow(TUnionType.UNION, "union_distinct_person_company").run(cjsc);

//        union(TUnionType.UNION_ALL, "union_all_person_person");

        union(TUnionType.UNION, "union_distinct_person_company");
    }

    private static void union(TUnionType unionType, String tableName) throws Exception {
//        IEngineMockService obj = (IEngineMockService) Naming.lookup("//" + ConfigurationUtil.getEngineRmiServerIP() + "/engineService");
//        TSquidFlow joinFlow =  createSquidFlow(unionType, tableName);
//        obj.callEngineJobMock(joinFlow);
//        System.out.println(obj.take(joinFlow.getTaskId(), 1111, 3));
//        System.out.println(obj.takeSample(joinFlow.getTaskId(), 1111, false, 6, 2));
//        Thread.sleep(1000 * 10);
//        System.out.println(obj.resumeEngine(joinFlow.getTaskId(), 1111));
    }

    private static TSquidFlow createSquidFlow(TUnionType unionType, String tableName) {
        TSquidFlow tsf = createSquidFlow(111, 222);
        tsf.setDebugModel(true);

        TDatabaseSquid personSquid = createPersonDatabaseSquid();

        TDatabaseSquid personSquid2 = createPersonDatabaseSquid2();

        TUnionSquid unionSquid = createUnionSquid(personSquid, personSquid2, unionType);

        TDataFallSquid dataFallSquid = createDataFallSquid(unionSquid, tableName);

        tsf.addSquid(personSquid);
        tsf.addSquid(personSquid2);
        tsf.addSquid(unionSquid);
        tsf.addSquid(createTDebugSquid(unionSquid, dataFallSquid));
        tsf.addSquid(dataFallSquid);

        return tsf;
    }

    private static TDataFallSquid createDataFallSquid(TUnionSquid unionSquid, String tableName) {
        TDataSource dataSource = new TDataSource("192.168.137.2", 3306,
                "squidflowtest", "root", "root", tableName, DataBaseType.MYSQL);

        TDataFallSquid dataFallSquid = new TDataFallSquid();
        dataFallSquid.setId("4");
        dataFallSquid.setSquidId(4);
        dataFallSquid.setDataSource(dataSource);
        Set<TColumn> columnSet = new HashSet<>();
        columnSet.add(new TColumn("person_id", 3, TDataType.LONG, true));
        columnSet.add(new TColumn("person_name", 4, TDataType.STRING, true));
        dataFallSquid.setColumnSet(columnSet);

        dataFallSquid.setPreviousSquid(unionSquid);
        return dataFallSquid;
    }

    private static TUnionSquid createUnionSquid(TDatabaseSquid left, TDatabaseSquid right, TUnionType unionType) {
        TUnionSquid unionSquid = new TUnionSquid();
        unionSquid.setLeftSquid(left);
        unionSquid.setRightSquid(right);
        unionSquid.setId("3");
        unionSquid.setSquidId(3);
        unionSquid.setUnionType(unionType);
        unionSquid.setLeftInKeyList(asList(1, 2));
        unionSquid.setRightInKeyList(asList(3, 4));
        return unionSquid;
    }

    private static TDatabaseSquid createCompanyDatabaseSquid() {
        TDataSource dataSource = new TDataSource("192.168.137.2", 3306,
                "squidflowtest", "root", "root", "company", DataBaseType.MYSQL);

        TDatabaseSquid companySquid = new TDatabaseSquid();
        companySquid.setId("2");
        companySquid.setSquidId(2);
        companySquid.setDataSource(dataSource);
        companySquid.putColumn(new TColumn("id", 3, TDataType.LONG));
        companySquid.putColumn(new TColumn("name", 4, TDataType.STRING));

        return companySquid;
    }

    private static TDatabaseSquid createPersonDatabaseSquid() {
        TDataSource dataSource = new TDataSource("192.168.137.2", 3306,
                "squidflowtest", "root", "root", "person", DataBaseType.MYSQL);

        TDatabaseSquid personSquid = new TDatabaseSquid();
        personSquid.setId("1");
        personSquid.setSquidId(1);
        personSquid.setDataSource(dataSource);
        personSquid.putColumn(new TColumn("id", 1, TDataType.LONG));
        personSquid.putColumn(new TColumn("name", 2, TDataType.STRING));

        return personSquid;
    }

    private static TDatabaseSquid createPersonDatabaseSquid2() {
        TDataSource dataSource = new TDataSource("192.168.137.2", 3306,
                "squidflowtest", "root", "root", "person2", DataBaseType.MYSQL);

        TDatabaseSquid personSquid = new TDatabaseSquid();
        personSquid.setId("2");
        personSquid.setSquidId(2);
        personSquid.setDataSource(dataSource);
        personSquid.putColumn(new TColumn("id", 3, TDataType.LONG));
        personSquid.putColumn(new TColumn("name", 4, TDataType.STRING));

        return personSquid;
    }
}
