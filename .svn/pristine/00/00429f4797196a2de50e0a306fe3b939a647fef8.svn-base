package com.eurlanda.datashire.engine.entity.transformation;

import com.eurlanda.datashire.engine.entity.DataCell;
import com.eurlanda.datashire.engine.entity.TTransformation;
import com.eurlanda.datashire.engine.entity.TTransformationAction;
import com.eurlanda.datashire.engine.enumeration.TTransformationInfoType;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.engine.util.DSUtil;
import com.eurlanda.datashire.engine.util.DataCellUtil;
import com.eurlanda.datashire.engine.util.SquidUtil;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by zhudebin on 15-5-6.
 */
public class PredictWithKeyProcessor extends TransformationProcessor {

    // 待做的transformation 操作
    private TTransformationAction tTransformationAction;

    public PredictWithKeyProcessor(JavaRDD<Map<Integer, DataCell>> inputRDD, TTransformationAction tTransformationAction) {
        this.inputRDD = inputRDD;
        this.tTransformationAction = tTransformationAction;
    }

    @Override
    public JavaRDD<Map<Integer, DataCell>> process(JavaSparkContext jsc) {
        ArrayList<JavaRDD<Map<Integer, DataCell>>> resultRDDList = new ArrayList();
        TTransformation tTransformation = tTransformationAction.gettTransformation();
        Map<String, Object> infoMap = tTransformation.getInfoMap();
        List<Integer> inKeys = tTransformation.getInKeyList();
        String sql = (String) infoMap.get(TTransformationInfoType.PREDICT_DM_MODEL.dbValue);
        // 模型类型
        SquidTypeEnum modelSquidTypeEnum = (SquidTypeEnum) infoMap.get(TTransformationInfoType.PREDICT_MODEL_TYPE.dbValue);
        // 获取key
        final int keyId = tTransformation.getInKeyList().get(1);
        JavaRDD<Map<Integer, DataCell>> preRDD = inputRDD.persist(StorageLevel.MEMORY_AND_DISK());
        List<DataCell> keyList = preRDD.map(new Function<Map<Integer, DataCell>, DataCell>() {
            @Override
            public DataCell call(Map<Integer, DataCell> v1) throws Exception {
                return v1.get(keyId);
            }
        }).distinct().collect();

        final int rmKey = tTransformationAction.getRmKeys() == null ? 0 :
                (tTransformationAction.getRmKeys().size()>0?
                        tTransformationAction.getRmKeys().toArray(new Integer[0])[0]:0);
        for (final DataCell keyDC : keyList) {
            JavaRDD<Map<Integer, DataCell>> filterRDD = preRDD.filter(new Function<Map<Integer, DataCell>, Boolean>() {
                @Override
                public Boolean call(Map<Integer, DataCell> v1) throws Exception {
                    if (DSUtil.isNull(keyDC)) {
                        return DSUtil.isNull(v1.get(keyId));
                    } else {
                        return keyDC.equals(v1.get(keyId));
                    }
                }
            });
            // 存在key
            List<byte[]> mbytes = null;
            if(DSUtil.isNull(keyDC)) {
                mbytes = ConstantUtil.getDataMiningJdbcTemplate().query(sql.replace("= ?"," is null "), new RowMapper<byte[]>() {
                    @Override
                    public byte[] mapRow(ResultSet rs, int rowNum) throws SQLException {
                        return rs.getBytes(1);
                    }
                });
            } else {
                mbytes = ConstantUtil.getDataMiningJdbcTemplate().query(sql, new RowMapper<byte[]>() {
                    @Override
                    public byte[] mapRow(ResultSet rs, int rowNum) throws SQLException {
                        return rs.getBytes(1);
                    }
                }, new Object[]{DataCellUtil.getData(keyDC)});
            }
            if(mbytes.size()==0) {
                throw new RuntimeException("该数据挖掘模型不存在,key为:" + DataCellUtil.getData(keyDC));
            }
            byte[] bytes = mbytes.get(0);
          //  Broadcast modelObj = jsc.broadcast(SquidUtil.genModel(bytes, squidTypeEnum));
            Object modelObj = SquidUtil.genModel(bytes, modelSquidTypeEnum);
            JavaRDD<Map<Integer, DataCell>> predictRDD ;
            if(tTransformation.getType().equals(TransformationTypeEnum.INVERSENORMALIZER)){
                predictRDD = inverseNormalizer(filterRDD, modelObj, inKeys.get(0),
                              tTransformation.getOutKeyList().get(0));
            }else {
                 predictRDD = predict(filterRDD, modelObj, modelSquidTypeEnum, inKeys.get(0),
                         tTransformation.getOutKeyList().get(0), rmKey);
            }
            if(predictRDD != null && ! predictRDD.isEmpty()) {
                resultRDDList.add(predictRDD);
            }else {
                throw new RuntimeException("预测数据异常");
            }
        }
        if(resultRDDList.size() == 0){
            return jsc.parallelize(new ArrayList<Map<Integer, DataCell>>());
        }else {
            return jsc.union(resultRDDList.get(0), resultRDDList.subList(1, resultRDDList.size()));
        }
    }

    private JavaRDD<Map<Integer, DataCell>> inverseNormalizer(JavaRDD<Map<Integer, DataCell>> inRDD,
                                                              final Object modelObj,final int inkey, final int outKey) {
     //   final NormalizerModel normalizerModel = (NormalizerModel) modelObj;
        return inRDD.map(new Function<Map<Integer, DataCell>, Map<Integer, DataCell>>() {
            @Override
            public Map<Integer, DataCell> call(Map<Integer, DataCell> v1) throws Exception {
              /*  DataCell dataCell = v1.get(inkey);
                DataCell result;
                if (dataCell == null) {
                    result = new DataCell(TDataType.STRING,"数据是null");
                    v1.put(outKey, result);
                }
                String data = dataCell.getData().toString();
                if (data == null || data.trim().equals("")) {
                    result = new DataCell(TDataType.STRING,"数据是null或是空字符串");
                    v1.put(outKey, result);
                } else {
                    result = new DataCell(TDataType.CSN, normalizerModel.inverseNormalizer(data));
                }
                v1.put(outKey, result);
                return v1; */
                DataCell result = DSUtil.inverseNormalizer(modelObj, v1.get(inkey));
                v1.put(outKey, result);
                return v1;
            }
        });
    }

    private JavaRDD<Map<Integer, DataCell>> predict(JavaRDD<Map<Integer, DataCell>> inRDD, final Object modelObj,
                                                    final SquidTypeEnum squidTypeEnum, final int inkey,
                                                    final int outKey, final int removeKey) {
       /* return inRDD.map(new Function<Map<Integer, DataCell>, Map<Integer, DataCell>>() {
            @Override
            public Map<Integer, DataCell> call(Map<Integer, DataCell> v1) throws Exception {
                DataCell result = DSUtil.predict(modelObj, squidTypeEnum, v1.get(inkey));
                v1.put(outKey, result);
                // remove key
                if (removeKey > 0) {
                    v1.remove(removeKey);
                }
                return v1;
            }
        });*/
     /*  JavaRDD<Map<Integer, DataCell>> filterRdd = inRDD.filter(new Function<Map<Integer, DataCell>, Boolean>() {
            @Override
            public Boolean call(Map<Integer, DataCell> v1) {
                return v1.containsKey(inkey);
            }
        }); */
        if (SquidTypeEnum.parse(squidTypeEnum.value()) == SquidTypeEnum.ALS) {
            JavaRDD<Map<Integer, DataCell>> result = DSUtil.predict(modelObj, squidTypeEnum, inputRDD, inkey, outKey);
            return result;
        } else {
            return inRDD.map(new Function<Map<Integer, DataCell>, Map<Integer, DataCell>>() {
                @Override
                public Map<Integer, DataCell> call(Map<Integer, DataCell> v1) throws Exception {
                    DataCell result = DSUtil.predict(modelObj, squidTypeEnum, v1.get(inkey));
                    v1.put(outKey, result);
                   // if (removeKey > 0) {
                   //   v1.remove(removeKey);
                   //  }
                    return v1;
                }
            });
        }
    }
}
