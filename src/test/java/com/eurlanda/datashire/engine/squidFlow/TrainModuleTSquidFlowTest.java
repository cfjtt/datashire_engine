package com.eurlanda.datashire.engine.squidFlow;

import com.eurlanda.datashire.engine.entity.TColumn;
import com.eurlanda.datashire.engine.entity.TDataSource;
import com.eurlanda.datashire.engine.entity.TDataType;
import com.eurlanda.datashire.engine.entity.TDatabaseSquid;
import com.eurlanda.datashire.engine.entity.TSquidFlow;
import com.eurlanda.datashire.engine.entity.TTransformation;
import com.eurlanda.datashire.engine.entity.TTransformationSquid;
import com.eurlanda.datashire.engine.enumeration.TTransformationInfoType;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import org.apache.spark.CustomJavaSparkContext;

import java.util.HashMap;

import static com.eurlanda.datashire.engine.util.ConfigurationUtil.*;

/**
 * 模型训练 squid flow
 * Created by Juntao.Zhang on 2014/4/18.
 */
public class TrainModuleTSquidFlowTest extends AbstractSquidTest {
    public static void main(String[] args) throws Exception {
        CustomJavaSparkContext cjsc = new CustomJavaSparkContext(getSparkMasterUrl(), "TrainModuleTSquidFlowTest", getSparkHomeDir(), getSparkJarLocation());
        getTrainModuleOfTSquidFlow().run(cjsc);
    }

    /**
     * 训练模型 SquidFlow
     */
    public static TSquidFlow getTrainModuleOfTSquidFlow() {
        TSquidFlow squidFlow = createSquidFlow(40000, 40001);

        TDatabaseSquid databaseSquid = createDatabaseSquid();

        squidFlow.addSquid(databaseSquid);

        TTransformation transformationContentArray = new TTransformation();
        transformationContentArray.setId(1);
        transformationContentArray.setDbId(1);
        transformationContentArray.setType(TransformationTypeEnum.TOKENIZATION);
        HashMap<String, Object> infoMap = new HashMap<>();
        infoMap.put(TTransformationInfoType.DICT_SQUID.dbValue, getEmotionDic());
        transformationContentArray.setInfoMap(infoMap);
        transformationContentArray.setInKeyList(keyList(1, 2));
        transformationContentArray.setOutKeyList(keyList(3));

        TTransformationSquid transformationSquid = new TTransformationSquid();
        transformationSquid.setId("2");
        transformationSquid.setSquidId(2);
//        transformationSquid.addTransformationSequence(transformationSequence); //todo:juntao.zhang
        transformationSquid.setPreviousSquid(databaseSquid);
        squidFlow.addSquid(transformationSquid);

        return squidFlow;
    }

    private static TDatabaseSquid createDatabaseSquid() {
        TDatabaseSquid databaseSquid = new TDatabaseSquid();
        TDataSource dataSource = new TDataSource("192.168.137.2", 3306,
                "squidflowtest", "root", "root", "train_data_train", DataBaseType.MYSQL);
        databaseSquid.setId("1");
        databaseSquid.setSquidId(1);
        databaseSquid.setDataSource(dataSource);
        databaseSquid.putColumn(new TColumn("value", 1, TDataType.DOUBLE));
        databaseSquid.putColumn(new TColumn("content", 2, TDataType.STRING));
        return databaseSquid;
    }
}
