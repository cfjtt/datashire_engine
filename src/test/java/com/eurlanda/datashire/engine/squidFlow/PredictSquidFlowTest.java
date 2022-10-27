package com.eurlanda.datashire.engine.squidFlow;

import com.eurlanda.datashire.engine.entity.*;
import com.eurlanda.datashire.engine.enumeration.TTransformationInfoType;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import org.apache.spark.CustomJavaSparkContext;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static com.eurlanda.datashire.engine.util.ConfigurationUtil.*;

/**
 * 模型预测
 * Created by Juntao.Zhang on 2014/4/18.
 */
public class PredictSquidFlowTest extends AbstractSquidTest {
    public static void main(String[] args) throws Exception {
        getPredictFlow().run(new CustomJavaSparkContext(getSparkMasterUrl(), "test", getSparkHomeDir(), getSparkJarLocation()));
//        IEngineMockService service = RpcServerFactory.getEngineMockService();
//        service.callEngineJobMock(getPredictFlow());
    }

    /**
     * 预测 Flow
     */
    public static TSquidFlow getPredictFlow() throws Exception {
        TSquidFlow squidFlow = createSquidFlow(20000, 20001);
        TDatabaseSquid databaseSquid = createDatabaseSquid();
        squidFlow.addSquid(databaseSquid);

        //TTransformation squid
        TTransformationSquid transformationSquid = createTransformationSquid(databaseSquid);
        squidFlow.addSquid(transformationSquid);


        //落地squid
        TDataFallSquid dataFallSquid = createDataFallSquid(transformationSquid, "train_data_predict");
        squidFlow.getSquidList().add(dataFallSquid);

        //report squid
        TReportSquid reportSquid = new TReportSquid();
        reportSquid.setId("6");
        reportSquid.setSquidId(6);
        reportSquid.from(squidFlow);
        reportSquid.addPreSquid(dataFallSquid);
        squidFlow.getSquidList().add(reportSquid);

        return squidFlow;
    }

    private static TTransformationSquid createTransformationSquid(TDatabaseSquid databaseSquid) throws Exception {
        TTransformation transformationValue = new TTransformation();
        transformationValue.setId(1);
        transformationValue.setDbId(1);
        transformationValue.setType(TransformationTypeEnum.PREDICT);
        HashMap<String, Object> infoMap = new HashMap<>();
        infoMap.put(TTransformationInfoType.DICT_SQUID.dbValue, getEmotionDic());
        infoMap.put(TTransformationInfoType.TRAIN_MODEL_TYPE.dbValue, getNaiveBayesModel());
        transformationValue.setInfoMap(infoMap);
        transformationValue.setInKeyList(keyList(2));
        transformationValue.setOutKeyList(keyList(6));

        TTransformation transformationId = new TTransformation();
        transformationId.setId(2);
        transformationId.setDbId(1);
        transformationId.setType(TransformationTypeEnum.VIRTUAL);
        transformationId.setInKeyList(keyList(1));
        transformationId.setOutKeyList(keyList(5));
        TTransformation transformationContent = new TTransformation();
        transformationContent.setId(3);
        transformationContent.setDbId(1);
        transformationContent.setType(TransformationTypeEnum.VIRTUAL);
        transformationContent.setInKeyList(keyList(2));
        transformationContent.setOutKeyList(keyList(7));
        TTransformationSquid transformationSquid = new TTransformationSquid();
        transformationSquid.setId("2");
        transformationSquid.setSquidId(2);
//        transformationSquid.addTransformationSequence(transformationSequence);//todo:juntao.zhang
        transformationSquid.setPreviousSquid(databaseSquid);
        return transformationSquid;
    }

    private static TDataFallSquid createDataFallSquid(TTransformationSquid transformationSquid, String tableName) {
        TDataSource dataSource = new TDataSource("192.168.137.2", 3306,
                "squidflowtest", "root", "root", tableName, DataBaseType.MYSQL);

        TDataFallSquid dataFallSquid = new TDataFallSquid();
        dataFallSquid.setId("5");
        dataFallSquid.setSquidId(5);
        Set<TColumn> columnSet = new HashSet<>();
        columnSet.add(new TColumn("id", 5, TDataType.LONG));
        columnSet.add(new TColumn("train_value", 6, TDataType.DOUBLE));
        columnSet.add(new TColumn(7, "content", TDataType.STRING, true, 20000, 0));

        dataFallSquid.setColumnSet(columnSet);
        dataFallSquid.setDataSource(dataSource);
        dataFallSquid.setPreviousSquid(transformationSquid);
        return dataFallSquid;
    }

    private static TDatabaseSquid createDatabaseSquid() {
        TDataSource dataSource = new TDataSource("192.168.137.2", 3306,
                "squidflowtest", "root", "root", "train_data_test", DataBaseType.MYSQL);
        //数据源 squid
        TDatabaseSquid databaseSquid = new TDatabaseSquid();
        databaseSquid.setId("1");
        databaseSquid.setSquidId(1);
        databaseSquid.setDataSource(dataSource);
        databaseSquid.putColumn(new TColumn("id", 1, TDataType.LONG));
        databaseSquid.putColumn(new TColumn("content", 2, TDataType.STRING));
        databaseSquid.putColumn(new TColumn("test_value", 3, TDataType.DOUBLE));
        databaseSquid.putColumn(new TColumn("train_value", 4, TDataType.DOUBLE));
        return databaseSquid;
    }
}
