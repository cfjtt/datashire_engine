package com.eurlanda.datashire.engine.squidFlow;

import com.eurlanda.datashire.engine.entity.*;
import com.eurlanda.datashire.engine.enumeration.TTransformationInfoType;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import org.apache.spark.CustomJavaSparkContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.eurlanda.datashire.engine.util.ConfigurationUtil.*;

/**
 * 各种 join squid
 * Created by Juntao.Zhang on 2014/4/18.
 */
public class TransformationSquidTest extends AbstractSquidTest {
    public static void main(String[] args) throws Exception {
        launch("student_transformation_result");
    }

    private static void launch(String tableName) throws Exception {
        createSquidFlow(tableName).run(new CustomJavaSparkContext
                (getSparkMasterUrl(), "SquidFlowLauncher", getSparkHomeDir(), getSparkJarLocation()));
//        SquidFlowLauncher.launch(createSquidFlow(tableName));
//        createSquidFlow(tableName).run(new CustomJavaSparkContext(getSparkMasterUrl(), "test", getSparkHomeDir(), getSparkJarLocation()));
    }

    public static TSquidFlow createSquidFlow(String tableName) throws Exception {
        TSquidFlow squidFlow = createSquidFlow(3, 30011);

        TDatabaseSquid studentSquid = createStudentDatabaseSquid();

        TTransformationSquid transformationSquid = createTransformationSquid(studentSquid);

        TDataFallSquid dataFallSquid = createDataFallSquid(transformationSquid, tableName);

//        TDataFallSquid exceptionDataFallSquid = createExceptionDataFallSquid(transformationSquid, tableName + "_exception");

        squidFlow.addSquid(studentSquid);
        squidFlow.addSquid(transformationSquid);
        squidFlow.addSquid(dataFallSquid);
//        squidFlow.addSquid(exceptionDataFallSquid);
        return squidFlow;
    }

    private static TTransformationSquid createTransformationSquid(TDatabaseSquid databaseSquid) throws Exception {
        List<TTransformationAction> actionList = new ArrayList<>();

        TTransformation name = new TTransformation();
        name.setId(2);
        name.setDbId(1);
        name.setType(TransformationTypeEnum.UPPER);
        name.setInKeyList(keyList(2));
        name.setOutKeyList(keyList(12));
        TTransformationAction action = new TTransformationAction();
        action.setRmKeys(keySet(1, 2));
        action.settTransformation(name);
        actionList.add(action);

        TTransformation com_name = new TTransformation();
        com_name.setId(3);
        com_name.setDbId(1);
        com_name.setType(TransformationTypeEnum.LOWER);
        com_name.setInKeyList(keyList(3));
        com_name.setOutKeyList(keyList(13));
        action = new TTransformationAction();
        action.setRmKeys(keySet(3));
        action.settTransformation(com_name);
        actionList.add(action);

        TTransformation choice = new TTransformation();
        choice.setId(4);
        choice.setDbId(1);
        choice.setType(TransformationTypeEnum.CHOICE);
        choice.setInKeyList(keyList(4, 5, 6));
        choice.setOutKeyList(keyList(14));
//        choice.setExpressionLists(new ArrayList<List<TFilterExpression>>());
//        choice.getExpressionLists().add(createFilter(4, 5, 6, TExpOperationEnum.GE));
//        choice.getExpressionLists().add(createFilter(5, 4, 6, TExpOperationEnum.GE));
//        choice.getExpressionLists().add(createFilter(6, 4, 5, TExpOperationEnum.GE));
        action = new TTransformationAction();
        action.settTransformation(choice);
        actionList.add(action);

        choice = new TTransformation();
        choice.setId(5);
        choice.setDbId(1);
        choice.setType(TransformationTypeEnum.CHOICE);
        choice.setInKeyList(keyList(4, 5, 6));
        choice.setOutKeyList(keyList(15));
//        choice.setExpressionLists(new ArrayList<List<TFilterExpression>>());
//        choice.getExpressionLists().add(createFilter(4, 5, 6, TExpOperationEnum.LE));
//        choice.getExpressionLists().add(createFilter(5, 4, 6, TExpOperationEnum.LE));
//        choice.getExpressionLists().add(createFilter(6, 4, 5, TExpOperationEnum.LE));
        action = new TTransformationAction();
        action.setRmKeys(keySet(4, 5, 6));
        action.settTransformation(choice);
        actionList.add(action);

        TTransformation time = new TTransformation();
        time.setId(7);
        time.setDbId(1);
        time.setType(TransformationTypeEnum.FORMATDATE);
        time.setInKeyList(keyList(7));
        time.setOutKeyList(keyList(17));
        HashMap<String, Object> infoMap = new HashMap<>();
        infoMap.put(TTransformationInfoType.DATE_FORMAT.dbValue, "yyyy-MM-dd HH:mm:ss");
        time.setInfoMap(infoMap);
        action = new TTransformationAction();
        action.setRmKeys(keySet(7));
        action.settTransformation(time);
        actionList.add(action);

        TTransformationSquid transformationSquid = new TTransformationSquid();
        transformationSquid.setId("2");
        transformationSquid.setSquidId(2);
        transformationSquid.setExceptionHandle(true);
        transformationSquid.setPreviousSquid(databaseSquid);
        transformationSquid.settTransformationActions(actionList);
        return transformationSquid;
    }


    private static TDataFallSquid createDataFallSquid(TSquid preSquid, String tableName) {
        TDataSource dataSource = new TDataSource("192.168.137.2", 3306,
                "squidflowtest", "root", "root", tableName, DataBaseType.MYSQL);

        TDataFallSquid dataFallSquid = new TDataFallSquid();
        dataFallSquid.setId("4");
        dataFallSquid.setSquidId(4);

        Set<TColumn> tColumns = new HashSet<>();
        tColumns.add(new TColumn("id", 1, TDataType.LONG, true, true));
        tColumns.add(new TColumn("name", 12, TDataType.STRING, true));
        tColumns.add(new TColumn("company_name", 13, TDataType.STRING, true));
        tColumns.add(new TColumn(14, "max_score", TDataType.DOUBLE, true, 5, 2));
        tColumns.add(new TColumn(15, "min_score", TDataType.DOUBLE, true, 5, 2));
        tColumns.add(new TColumn("date_time", 17, TDataType.TIMESTAMP, true));

        dataFallSquid.setColumnSet(tColumns);
        dataFallSquid.setDataSource(dataSource);
        dataFallSquid.setPreviousSquid(preSquid);
        return dataFallSquid;
    }

    private static TDataFallSquid createExceptionDataFallSquid(TSquid preSquid, String tableName) {
        TDataSource dataSource = new TDataSource("192.168.137.2", 3306,
                "squidflowtest", "root", "root", tableName, DataBaseType.MYSQL);

        TDataFallSquid dataFallSquid = new TDataFallSquid();
        dataFallSquid.setId("4");
        dataFallSquid.setSquidId(4);

        Set<TColumn> tColumns = new HashSet<>();
        tColumns.add(new TColumn("id", 1, TDataType.LONG, true, true));
        tColumns.add(new TColumn("name", 2, TDataType.STRING));
        tColumns.add(new TColumn("company_name", 3, TDataType.STRING));
        tColumns.add(new TColumn("chinese_score", 4, TDataType.DOUBLE));
        tColumns.add(new TColumn("english_score", 5, TDataType.DOUBLE));
        tColumns.add(new TColumn("math_score", 6, TDataType.DOUBLE));
        tColumns.add(new TColumn("date_time", 7, TDataType.STRING));

        dataFallSquid.setColumnSet(tColumns);
        dataFallSquid.setDataSource(dataSource);
        dataFallSquid.setPreviousSquid(preSquid);
        return dataFallSquid;
    }

    private static TDatabaseSquid createStudentDatabaseSquid() {
        TDataSource dataSource = new TDataSource("192.168.137.2", 3306,
                "squidflowtest", "root", "root", "student", DataBaseType.MYSQL);
        TDatabaseSquid personSquid = new TDatabaseSquid();
        personSquid.setId("1");
        personSquid.setSquidId(1);
        personSquid.setDataSource(dataSource);
        personSquid.putColumn(new TColumn("id", 1, TDataType.LONG, true, true));
        personSquid.putColumn(new TColumn("name", 2, TDataType.STRING));
        personSquid.putColumn(new TColumn("company_name", 3, TDataType.STRING));
        personSquid.putColumn(new TColumn("chinese_score", 4, TDataType.DOUBLE));
        personSquid.putColumn(new TColumn("english_score", 5, TDataType.DOUBLE));
        personSquid.putColumn(new TColumn("math_score", 6, TDataType.DOUBLE));
        personSquid.putColumn(new TColumn("date_time", 7, TDataType.STRING));
        return personSquid;
    }

}
