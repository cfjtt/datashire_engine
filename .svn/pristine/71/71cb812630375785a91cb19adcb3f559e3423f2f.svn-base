package com.eurlanda.datashire.engine.squidFlow;

import com.eurlanda.datashire.engine.entity.TColumn;
import com.eurlanda.datashire.engine.entity.TDataFallSquid;
import com.eurlanda.datashire.engine.entity.TDataSource;
import com.eurlanda.datashire.engine.entity.TDataType;
import com.eurlanda.datashire.engine.entity.TDatabaseSquid;
import com.eurlanda.datashire.engine.entity.TSquidFlow;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.enumeration.DataBaseType;
import org.apache.spark.CustomJavaSparkContext;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhudebin on 15-1-8.
 */
public class PhoenixExctractSquidTest extends AbstractSquidTest {


    @Test
    public void test() {
        TSquidFlow tsf = createSquidFlow(1, 2);
        TDatabaseSquid tds = new TDatabaseSquid();
        TDataSource ds = new TDataSource();
        ds.setType(DataBaseType.HBASE_PHOENIX);
        ds.setHost("e223");
        ds.setPort(2181);
        ds.setTableName("NJ_LOAD_DATA_06");
        ds.setFilter("JGSJ BETWEEN TO_DATE('2014-06-01 00:00:13', 'yyyy-MM-dd HH:mm:ss', 'GMT+8') " +
                "AND TO_DATE('2014-07-07 00:11:13', 'yyyy-MM-dd HH:mm:ss', 'GMT+8') AND KKMC='柘汪新老204路口北' AND HPHM IS NOT NULL");
        tds.setDataSource(ds);
        // 待抽取的列
        Set<TColumn> columnSet = new HashSet<>();
        TColumn c_hphm = new TColumn();
        c_hphm.setData_type(TDataType.STRING);
        c_hphm.setName("HPHM");
        c_hphm.setId(1);
        columnSet.add(c_hphm);
        tds.setColumnSet(columnSet);

        tsf.addSquid(tds);

        TDataFallSquid dfs = new TDataFallSquid();
        TDataSource ds2 = new TDataSource();
        ds2.setType(DataBaseType.HBASE_PHOENIX);
        ds2.setHost("e223");
        ds2.setPort(2181);
        ds2.setTableName("NJ_LOAD_DATA_06_TEST");
        dfs.setDataSource(ds2);
        Set<TColumn> cs = new HashSet<>();
        TColumn c1 = new TColumn();
        c1.setData_type(TDataType.STRING);
        c1.setName("HPHM");
        c1.setId(1);
        c1.setPrimaryKey(true);
        cs.add(c1);
        dfs.setColumnSet(cs);
        dfs.setPreviousSquid(tds);
        tsf.addSquid(dfs);

        CustomJavaSparkContext cjsc = new CustomJavaSparkContext("local[8]", "DataShire-local-test");
        try {
            tsf.run(cjsc);
        } catch (EngineException e) {
            e.printStackTrace();
        }
    }

}
