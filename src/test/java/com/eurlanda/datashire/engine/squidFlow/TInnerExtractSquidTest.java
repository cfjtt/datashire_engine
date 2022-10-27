package com.eurlanda.datashire.engine.squidFlow;

import com.eurlanda.datashire.engine.entity.*;
import com.eurlanda.datashire.engine.util.ConfigurationUtil;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import org.apache.spark.CustomJavaSparkContext;

import java.util.HashSet;
import java.util.Set;

import static com.eurlanda.datashire.engine.util.ConfigurationUtil.*;

/**
 * Created by Juntao.Zhang on 2014/5/8.
 */
public class TInnerExtractSquidTest extends AbstractSquidTest {
    public static void main(String[] args) throws Exception {
        launch();
    }

    private static void launch() throws Exception {
        createSquidFlow().run(new CustomJavaSparkContext(getSparkMasterUrl(), "test", getSparkHomeDir(), getSparkJarLocation()));
    }

    public static TSquidFlow createSquidFlow() throws Exception {
        TSquidFlow squidFlow = createSquidFlow(30010, 30011);
        TDatabaseSquid personSquid = createPersonDatabaseSquid();
        TDataFallSquid dataFallSquid = createDataFallSquid("2", 2, personSquid, "PERSON_FALL_1");
        TInnerExtractSquid innerExtractSquid = createPerson2DatabaseSquid(dataFallSquid, "PERSON_FALL_1");
        TTransformationSquid tTransformationSquid = createTransformationSquid(innerExtractSquid);
        TDataFallSquid dataFallSquid2 = createDataFallSquid("6", 6, tTransformationSquid, "PERSON_FALL_2");
        squidFlow.addSquid(personSquid);
        squidFlow.addSquid(dataFallSquid);
        squidFlow.addSquid(innerExtractSquid);
        squidFlow.addSquid(tTransformationSquid);
        squidFlow.addSquid(dataFallSquid2);
        return squidFlow;
    }

    private static TTransformationSquid createTransformationSquid(TSquid preSquid) throws Exception {
        TTransformation id = new TTransformation();
        id.setId(1);
        id.setDbId(3);
        id.setType(TransformationTypeEnum.VIRTUAL);
        id.setInKeyList(keyList(4));
        id.setOutKeyList(keyList(1));
        TTransformation name = new TTransformation();
        name.setId(2);
        name.setDbId(3);
        name.setType(TransformationTypeEnum.UPPER);
        name.setInKeyList(keyList(5));
        name.setOutKeyList(keyList(2));
        TTransformation company_id = new TTransformation();
        company_id.setId(3);
        company_id.setDbId(3);
        company_id.setType(TransformationTypeEnum.VIRTUAL);
        company_id.setInKeyList(keyList(6));
        company_id.setOutKeyList(keyList(3));
        TTransformationSquid transformationSquid = new TTransformationSquid();
        transformationSquid.setId("10");
        transformationSquid.setSquidId(10);
//        transformationSquid.addTransformationSequence(transformationSequence);//todo:juntao.zhang
        transformationSquid.setPreviousSquid(preSquid);
        return transformationSquid;
    }

    private static TDatabaseSquid createPersonDatabaseSquid() {
        TDataSource dataSource = new TDataSource("192.168.137.2", 3306,
                "squidflowtest", "root", "root", "person", DataBaseType.MYSQL);

        TDatabaseSquid personSquid = new TDatabaseSquid();
        personSquid.setId("1");
        personSquid.setSquidId(1);
        personSquid.setDataSource(dataSource);
        personSquid.putColumn(new TColumn("id", 1, TDataType.LONG, true, true));
        personSquid.putColumn(new TColumn("name", 2, TDataType.STRING));
        personSquid.putColumn(new TColumn("company_id", 3, TDataType.LONG));

        return personSquid;
    }

    private static TInnerExtractSquid createPerson2DatabaseSquid(TSquid preSquid,String tableName) {
        TDataSource dataSource = new TDataSource(ConfigurationUtil.getInnerHbaseHost(),
                ConfigurationUtil.getInnerHbasePort(), "", "", "", tableName, DataBaseType.HBASE_PHOENIX);
        TInnerExtractSquid innerExtractSquid = new TInnerExtractSquid();
        TDatabaseSquid personSquid = new TDatabaseSquid();
        personSquid.setId("3");
        personSquid.setSquidId(3);
        personSquid.setDataSource(dataSource);
        personSquid.putColumn(new TColumn("id", 4, TDataType.LONG, true, true));
        personSquid.putColumn(new TColumn("name", 5, TDataType.STRING, true));
        personSquid.putColumn(new TColumn("company_id", 6, TDataType.LONG, true));
        innerExtractSquid.setPreTSquid(preSquid);
        innerExtractSquid.settDatabaseSquid(personSquid);
        innerExtractSquid.setId("4");
        innerExtractSquid.setSquidId(4);
        return innerExtractSquid;
    }

    private static TDataFallSquid createDataFallSquid(String id,int squidId,TSquid preSquid, String tableName) {
        TDataSource dataSource = new TDataSource(ConfigurationUtil.getInnerHbaseHost(),
                ConfigurationUtil.getInnerHbasePort(), "", "", "", tableName, DataBaseType.HBASE_PHOENIX);

        TDataFallSquid dataFallSquid = new TDataFallSquid();
        dataFallSquid.setId(id);
        dataFallSquid.setSquidId(squidId);
        dataFallSquid.setTruncateExistingData(true);

        Set<TColumn> columnSet = new HashSet<>();
        columnSet.add(new TColumn("id", 1, TDataType.LONG, true, true));
        columnSet.add(new TColumn("name", 2, TDataType.STRING, true));
        columnSet.add(new TColumn("company_id", 3, TDataType.LONG, true));

        dataFallSquid.setColumnSet(columnSet);
        dataFallSquid.setDataSource(dataSource);
        dataFallSquid.setPreviousSquid(preSquid);
        return dataFallSquid;
    }


}
