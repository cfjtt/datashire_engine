package com.eurlanda.datashire.engine.util;

import com.alibaba.fastjson.JSON;
import com.eurlanda.datashire.common.util.ConstantsUtil;
import com.eurlanda.datashire.engine.entity.*;
import com.eurlanda.datashire.engine.entity.transformation.AreaProxy;
import com.eurlanda.datashire.engine.enumeration.DifferenceType;
import com.eurlanda.datashire.engine.enumeration.TTransformationInfoType;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.exception.NotImplementedException;
import com.eurlanda.datashire.engine.exception.TransformationException;
import com.eurlanda.datashire.engine.spark.mllib.TrainDataConvertor;
import com.eurlanda.datashire.engine.spark.mllib.associationrules.RulesQuery;
import com.eurlanda.datashire.engine.spark.mllib.model.QuantifyModel;
import com.eurlanda.datashire.engine.spark.mllib.normalize.NormalizerModel;
import com.eurlanda.datashire.engine.translation.expression.ExpressionValidator;
import com.eurlanda.datashire.entity.TransformationInputs;
import com.eurlanda.datashire.entity.operation.IncUnitEnum;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.utility.EnumException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.jdbc.core.RowMapper;
import scala.collection.mutable.StringBuilder;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.eurlanda.datashire.engine.entity.TTransformationSquid.ERROR_KEY;
import static java.util.Arrays.asList;

//import com.mongodb.BasicDBList;
//import com.mongodb.BasicDBObject;
//import scala.collection.mutable.StringBuilder;


/**
 * 根据 TransformationTypeEnum math
 * Created by Juntao.Zhang on 2014/5/12.
 */
public enum TransformationTypeAdaptor implements Serializable {
    AUTO_INCREMENT {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            throw new NotImplementedException();
        }
    },
    UNKNOWN {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            throw new NotImplementedException();
        }
    },
    //起始列  客户端使用
    SOURCECOLUMN {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            throw new NotImplementedException();
        }
    },
    //终止列  客户端使用
    TARGETCOLUMN {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            throw new NotImplementedException();
        }
    },
    /**
     * Float  NumericCast(float, int)
     * 入参个数：2，说明：1：要转化的数值（必须是BIGINT	DECIMAL	DOUBLE	FLOAT	INT	REAL	SMALLINT	TINYINT	BIT之一）。
     * 2：目标类型的code（BIGINT	DECIMAL	DOUBLE	FLOAT	INT	REAL	SMALLINT	TINYINT	BIT之一）
     */
    NUMERICCAST {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (DSUtil.isNull(dc0)) {
                in.put(ConstantsUtil.ERROR_KEY, new DataCell(TDataType.STRING, "NUMERICCAST入参不能为null"));
                return;
            }
            DataCell resultDC = null;
            if (dc0 != null) {
                resultDC = dc0.clone();
            }
            Map<String, Object> infoMap = tTransformation.getInfoMap();
            Integer systemoutputDatatype = (Integer) infoMap.get(TTransformationInfoType.OUTPUT_DATA_TYPE.dbValue);
            if (dc0 != null && systemoutputDatatype != null) {
                Object number = null;
                if (!isInteger(getDataCell(1,inKeyList.get(1),in,tTransformation).getData().toString())) {
                    in.put(ConstantsUtil.ERROR_KEY, new DataCell(TDataType.STRING, "NUMERICCAST舍入的位数不是整数"));
                    return;
                }
                Integer scale = Integer.parseInt(getDataCell(1,inKeyList.get(1),in,tTransformation).getData().toString());
                if (scale < 0) {
                    in.put(ConstantsUtil.ERROR_KEY, new DataCell(TDataType.STRING, "NUMERICCAST舍入的位数不能是负数"));
                    return;
                }
                // Object number2 = ImplicitInstalledUtil.implicitInstalled(dc0.getdType(),SystemDatatype.parse(systemoutputDatatype),dc0.getData(),0,scale);
                TDataType newTdatatype = TDataType.sysType2TDataType(systemoutputDatatype);
                TDataType oldTdatetype = resultDC.getdType();
                if (newTdatatype != oldTdatetype) {
                    resultDC.setdType(newTdatatype);
                }
                BigDecimal bigDecimal = new BigDecimal(resultDC.getData().toString()).setScale(scale, BigDecimal.ROUND_HALF_UP);
                switch (resultDC.getdType()) {
                    case INT:
                        number = bigDecimal.intValue();
                        break;
                    case LONG:
                        number = bigDecimal.longValue();
                        break;
                    case DOUBLE:
                        number = bigDecimal.doubleValue();
                        break;
                    case FLOAT:
                        number = bigDecimal.floatValue();
                        break;
                    case BIG_DECIMAL:
                        number = bigDecimal;
                        break;
                    default:
                        in.put(ConstantsUtil.ERROR_KEY, new DataCell(TDataType.STRING, "NUMERICCAST不支持的数值类型转换"+resultDC));
                        return;
                }
                resultDC.setData(number);
                in.put(outKeyList.get(0), resultDC);
            }
        }
    },
    CSNTOSTRING {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            Integer outKey = tTransformation.getOutKeyList().get(0);
            DataCell dc = getDataCell(0,tTransformation.getInKeyList().get(0),in,tTransformation);
            in.put(outKey, new DataCell(TDataType.STRING, dc.getData()));
        }
    },
    INVERSEQUANTIFY {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            Integer outKey = tTransformation.getOutKeyList().get(0);
            Double inPara = Double.valueOf(getDataCell(0,tTransformation.getInKeyList().get(0),in,tTransformation)
                    .getData().toString());
            QuantifyModel quantifyModel = (QuantifyModel)tTransformation.getInfoMap()
                    .get(TTransformationInfoType.PREDICT_DM_MODEL.dbValue);
            if(quantifyModel ==null) {
                throw new IllegalArgumentException("量化模型不能是null");
            }
            in.put(outKey, quantifyModel.inverse(inPara));
        }
    },
    //虚拟
    VIRTUAL {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell dc0 = in.get(inKeyList.get(0));
            Map<String, Object> infoMap = tTransformation.getInfoMap();
            DataCell resultDC = null;
            if (dc0 != null) {
                resultDC = dc0.clone();
            }
            // outkey 小于 0时，为 referenceColumn上的virtTransformation,不需要验证数据
            // 1. 参与聚合的列也不需要验证
            if(outKeyList.get(0)<0
                    || infoMap.containsKey(TTransformationInfoType.SKIP_VALIDATE.dbValue)
                    || infoMap.containsKey(TTransformationInfoType.AGGREGATION_COLUMN.dbValue)) {
                in.put(outKeyList.get(0), resultDC);
                return;
            }
            //2. 是否为空判断
            if(DSUtil.isNull(resultDC)) {
                Object obj = infoMap.get(TTransformationInfoType.IS_NULLABLE.dbValue);
                Boolean iserror=(Boolean)obj;
                if(obj != null && ! iserror) {
                    logger.debug("异常数据,数据不能为null");
                    in.put(ConstantsUtil.ERROR_KEY, new DataCell(TDataType.STRING,"[数据校验]数据不能为Nullable"));
                    return;
                }else if(obj!=null&& iserror ){//如果值为空就返回空
                    Integer systemData = (Integer) infoMap.get(TTransformationInfoType.VIRT_TRANS_OUT_DATA_TYPE.dbValue);
                    resultDC = new DataCell(TDataType.sysType2TDataType(systemData), null);
                    in.put(outKeyList.get(0), resultDC);
                    return;
                }
            }
            // 3. 判断是否为hbase 抽取列
            if(infoMap.containsKey(TTransformationInfoType.HBASE_VARBINARY_CONVERT.dbValue)) {
                // 将varbinary 类型转换为 相应的输出类型
                Integer outDataType = (Integer)infoMap.get(TTransformationInfoType.OUTPUT_DATA_TYPE.dbValue);
                try {
                    if (DSUtil.isNotNull(resultDC)) {
                        TDataType tdt = TDataType.sysType2TDataType(outDataType);
                        resultDC.setdType(tdt);
                        switch (tdt) {
                        case BIG_DECIMAL:
                            resultDC.setData(Bytes.toBigDecimal((byte[]) resultDC.getData()));
                            break;
                        case BOOLEAN:
                            resultDC.setData(Bytes.toBoolean((byte[]) resultDC.getData()));
                            break;
                        case TINYINT:
                            resultDC.setData(((Short)Bytes.toShort((byte[])resultDC.getData())).byteValue());
                            break;
                        case SHORT:
                            resultDC.setData(Bytes.toShort((byte[])resultDC.getData()));
                            break;
                        case INT:
                            resultDC.setData(Bytes.toInt((byte[]) resultDC.getData()));
                            break;
                        case DOUBLE:
                            resultDC.setData(Bytes.toDouble((byte[]) resultDC.getData()));
                            break;
                        case FLOAT:
                            resultDC.setData(Bytes.toFloat((byte[]) resultDC.getData()));
                            break;
                        case LONG:
                            resultDC.setData(Bytes.toLong((byte[]) resultDC.getData()));
                            break;
                        case STRING:
                            resultDC.setData(Bytes.toString((byte[]) resultDC.getData()));
                            break;
                        case VARBINARY:
                            break;
                        default:
                            in.put(ConstantsUtil.ERROR_KEY, new DataCell(TDataType.STRING,
                                    "不能将varbinary类型转换为" + SystemDatatype.parse(outDataType).name()));
                            return;
                        }
                    }
                } catch (Exception e) {
                    in.put(ConstantsUtil.ERROR_KEY, new DataCell(TDataType.STRING,
                            "不能将varbinary类型转换为" + SystemDatatype.parse(outDataType).name()));
                    return;
                }
            }

            // 4. 判断是否要转换类型
            if(infoMap.containsKey(TTransformationInfoType.DOC_VARCHAR_CONVERT.dbValue)) {
                Integer outDataType = (Integer)infoMap.get(TTransformationInfoType.OUTPUT_DATA_TYPE.dbValue);
                try {
                    if (DSUtil.isNotNull(resultDC)) {
                        TDataType tdt = TDataType.sysType2TDataType(outDataType);
                        resultDC.setdType(tdt);
                        String data=(String)resultDC.getData();
                        boolean isNullOrEmpty = data.equalsIgnoreCase("null") || data.length() == 0;
                        switch (tdt) {
                            case STRING:
                                resultDC.setData(data);
                                break;
                            case TINYINT:
                                if(isNullOrEmpty){ // null时仍要转换为null的Int
                                    resultDC.setData(null);
                                }else {
                                    resultDC.setData(Byte.parseByte(data));
                                }
                                break;
                            case SHORT:
                                if(isNullOrEmpty){ // null时仍要转换为null的Int
                                    resultDC.setData(null);
                                }else {
                                    resultDC.setData(Short.parseShort(data));
                                }
                                break;
                            case INT:
                                if(isNullOrEmpty){ // null时仍要转换为null的Int
                                    resultDC.setData(null);
                                }else {
                                    resultDC.setData(Integer.parseInt(data));
                                }
                                break;
                            case LONG:
                                if(isNullOrEmpty){ // null时仍要转换为null的Double
                                    resultDC.setData(null);
                                }else {
                                    Long l = Long.parseLong(data);
                                    resultDC.setData(l);
                                }
                                break;
                            case FLOAT:
                                if(isNullOrEmpty){ // null时仍要转换为null的Double
                                    resultDC.setData(null);
                                }else {
                                    Float f = Float.parseFloat(data);
                                    resultDC.setData(f);
                                }
                                break;
                            case DOUBLE:
                                if(isNullOrEmpty){ // null时仍要转换为null的Double
                                    resultDC.setData(null);
                                }else {
                                    Double d = Double.parseDouble(data);
                                    resultDC.setData(d);
                                }
                                break;
//                            case TIME:
                            case TIMESTAMP:
                                if(isNullOrEmpty){ // null时仍要转换
                                    resultDC.setData(null);
                                }else {
                                    resultDC.setData(TransformationTypeAdaptor.StringToTimestampWithNaso(data));
                                }
                                break;
                            case BOOLEAN:
                                resultDC.setData(data.equalsIgnoreCase("true")
                                        || data.equals("1")
                                        || data.equalsIgnoreCase("y"));
                                break;
                            case BIG_DECIMAL:
                                if(isNullOrEmpty){ // null时仍要转换为null的BigDecimal
                                    resultDC.setData(null);
                                }else {
                                    resultDC.setData(new BigDecimal(data));
                                }
                                break;
                            case VARBINARY:
                                if(isNullOrEmpty){ // null时仍要转换
                                    resultDC.setData(null);
                                }else {
                                    resultDC.setData(data.getBytes());
                                }
                                break;
                            case DATE:
                                if(isNullOrEmpty){ // null时仍要转换
                                    resultDC.setData(null);
                                }else {
                                    resultDC.setData(DateUtil.util2sql(DateUtil.parseByDefaultDateFormat(data)));
                                }
                                break;
                            case ARRAY:
                                if(isNullOrEmpty){ // null时仍要转换
                                    resultDC.setData(null);
                                }else {
                                    resultDC.setData(parseArray(data));
                                }
                                break;
                            case MAP:
                                if(isNullOrEmpty){ // null时仍要转换
                                    resultDC.setData(null);
                                }else {
                                    resultDC.setData(parseMap(data));
                                }
                                break;
                            default:
                                in.put(ConstantsUtil.ERROR_KEY, new DataCell(TDataType.STRING,
                                        "不能将\""+data+"\"转换为" + SystemDatatype.parse(outDataType).name()+"类型"));
                                return;
                        }
                    }
                } catch (Exception e) {
                    in.put(ConstantsUtil.ERROR_KEY, new DataCell(TDataType.STRING,
                            "不能将类型转换为" + SystemDatatype.parse(outDataType).name()));
                    return;
                }
            }
            // 4. 精度判断
            /**
             * 1.string 字符串长度
//             * 2.float,double  precision [不需要]
             * 3.bigdecimal  precision,scale
             */
            if(resultDC != null) {
                Integer systemData = (Integer) infoMap.get(TTransformationInfoType.VIRT_TRANS_OUT_DATA_TYPE.dbValue);
                if(systemData!=null) {
                    switch (SystemDatatype.parse(systemData)) {//转换
//                        case TINYINT:
//                        case INT:
//                        case BIGINT:
//                        case BIT:
                        case DECIMAL:
//                        case FLOAT:
//                        case DOUBLE:
//                        case SMALLINT:
                            Object number = null;
                            Integer precision=null;
                            Integer scale = null;
                            if(infoMap.get(TTransformationInfoType.NUMERIC_PRECISION.dbValue)!=null){
                                precision = (Integer) infoMap.get(TTransformationInfoType.NUMERIC_PRECISION.dbValue);
                            }
                            if(TTransformationInfoType.NUMERIC_SCALE.dbValue!=null){
                                scale = (Integer) infoMap.get(TTransformationInfoType.NUMERIC_SCALE.dbValue);
                            }
                            number = ImplicitInstalledUtil.implicitInstalled(dc0.getdType(), SystemDatatype.parse(systemData), dc0.getData(), precision, scale);
                            resultDC.setdType(TDataType.sysType2TDataType(systemData));
                            resultDC.setData(number);
                            break;
                        default:
                    }
                }
               switch (resultDC.getdType()) {
                    case STRING:
                        // 字符串长度判断
                        Integer stringMaxLength = (Integer)infoMap.get(TTransformationInfoType.STRING_MAX_LENGTH.dbValue);
                        if(stringMaxLength == -1) {
                            // 字符串最大,do nothing
                        } else {
                            int stringLength = 0;
                            if(resultDC.getData() != null) {
                                stringLength = (resultDC.getData().toString()).length();
                            }
                            if(stringMaxLength<stringLength) {
                                String errmsg="字符串长度过大，最大值" + stringMaxLength + "<长度" + stringLength;
                                logger.debug("异常数据" + resultDC + ","+errmsg);
                                in.put(ERROR_KEY, new DataCell(TDataType.STRING, errmsg));
                                return;
                            }
                        }
                        break;
                    default:
                }
            }
            in.put(outKeyList.get(0), resultDC);
        }

        private boolean validateDouble(double d, Map<String, Object> infoMap, Map<Integer, DataCell> in) {
            Integer precision = (Integer)infoMap.get(
                    TTransformationInfoType.NUMERIC_PRECISION.dbValue);
            if(d - MathUtil.maxDouble(precision)>0) { // 超过最大值，异常
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "数据精度异常=>precision:" + precision + ",data:" + d));
                logger.debug("数据精度异常=>precision:" + precision + ",data:" + d);
                return false;
            }
            return true;
        }
    },
    //大写
    UPPER {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);;
            if (dc0 != null) {
                DataCell dc0Copy = dc0.clone();
                dc0Copy.setData(dc0Copy.getData()==null? null : dc0Copy.getData().toString().toUpperCase());
                in.put(outKeyList.get(0), dc0Copy);
            } else {
                in.put(outKeyList.get(0), null);
            }
        }
    },
    //串连
    CONCATENATE {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            StringBuilder sb = new StringBuilder();
            Map<String,Object> infoMap = tTransformation.getInfoMap();
            // 已sqlserver为标准
            for (Integer inkey : inKeyList) {
                Object data = in.get(inkey);
                if(data != null) {
                    Object v = in.get(inkey).getData();
                    if(v != null) {
                        sb.append(v.toString());
                    }
                }/*else { // 凡是和null做任何trans，其结果统一为null
                    in.put(outKeyList.get(0), null);
                    return;
                }*/
                if (inKeyList.indexOf(inkey) < inKeyList.size() - 1) {
                    if (infoMap.get(TTransformationInfoType.CONCATENATE_CONNECTOR.dbValue) != null) {
                        sb.append(infoMap.get(TTransformationInfoType.CONCATENATE_CONNECTOR.dbValue));
                    }

                }
            }
            DataCell dc = new DataCell();
            dc.setdType(TDataType.STRING);
            dc.setData(sb.toString());
            in.put(outKeyList.get(0), dc);
        }
    },
    //小写
    LOWER {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 != null) {
                DataCell dc0Copy = dc0.clone();
                dc0Copy.setData(dc0Copy.getData()==null? null : dc0Copy.getData().toString().toLowerCase());
                in.put(outKeyList.get(0), dc0Copy);
            } else {
                in.put(outKeyList.get(0), null);
            }
        }
    },
    // 常量
    // 定义一个常量，可以是不同类型。
    CONSTANT {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            TDataType dataType = (TDataType) tTransformation.getInfoMap().get(TTransformationInfoType.OUTPUT_DATA_TYPE.dbValue);
            Object value = tTransformation.getInfoMap().get(TTransformationInfoType.CONSTANT_VALUE.dbValue);
            Integer outKey = tTransformation.getOutKeyList().get(0);
            DataCell dc = new DataCell(dataType, value);
            if(dataType == TDataType.CSV ){ // 去掉 CSV 类型前后的单引号
                dc.setData(removeStartAndEndSingleQuotes(dc.getData().toString()));
            }else if(dataType == TDataType.VARBINARY){
                //不需要去掉前后的单引号
                dc.setData(value);
//                dc.setData(removeStartAndEndSingleQuotes((byte[])value));
            }
            in.put(outKey, dc);
        }
    },
    //多选一
    //inKeyList list 与 expressionList list 一一对应
    // 详情见 TransformationTypeAdaptorTest.testChoice
    CHOICE { //选择满足表达式条件的所有元素
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            List<Integer> inKeyList = tTransformation.getInKeyList();
            int outKey = outKeyList.get(0);
            List<TFilterExpression> inputsFilter = tTransformation.getInputsFilter();
          /*  for (int i = 0; i < inputsFilter.size() ; i++) {
                TFilterExpression fe = inputsFilter.get(i);
                if (fe != null) {
                    tTransformation.setFilterExpression(fe);
                    boolean validateres = ExpressionValidator.validate(fe, in);
                    if (validateres) {
                        in.put(outKey, in.get(inKeyList.get(i)).clone());
                    }
                }
            }*/
            int validateresidx = -1;//第几个条件成立
            for (int i = 0; i < inputsFilter.size(); i++) { //每条数据都要判断所有的条件
                TFilterExpression fe = inputsFilter.get(i);
                if (fe != null) {
                    boolean validateres = ExpressionValidator.validate(fe, in);
                    if (validateres) {
                        validateresidx = i;
                        break;
                    }
                }
            }
            if (validateresidx == -1) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "不满足所有条件"));
            }else{
                in.put(outKey, getDataCell(validateresidx,inKeyList.get(validateresidx),in,tTransformation).clone());
            }
        }
    },
    //从输入字符串的左边开始搜索，提取出和RegExpression匹配的第TermIndex个字串。如果没有，则返回空。
    //info:list
    //0:RegExpression
    //1:TermIndex
    TERMEXTRACT {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            DataCell result = new DataCell(TDataType.STRING, null);
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if(DSUtil.isNull(dc0)){
                throw new IllegalArgumentException("要分析的源字符串不能是null");
            }
            Object regExpression = getDataCell(1,inKeyList.get(1),in,tTransformation).getData();
            Object termIndex = getDataCell(2,inKeyList.get(2),in,tTransformation).getData();
            if (regExpression == null) {
                throw new IllegalArgumentException("表达式参数不能是null");
            }
            if (termIndex == null) {
                throw new IllegalArgumentException("提取索引不能是null");
            }
            Pattern p = null;
            try{
               p = Pattern.compile((String) regExpression);
            } catch (Exception e){
                throw new IllegalArgumentException("表达式参数不合法");
            }
            String str = dc0.getData().toString();
            Matcher m = p.matcher(str);
            int current = Integer.valueOf(termIndex.toString());
            int idx = 0;
            while (m.find()) {
                if(idx == current) {
                    result.setData(m.group(0));
                    in.put(outKeyList.get(0), result);
                    return;
                }
                idx++;
            }
           /* if(idx==0) { //测试要求：原数组中没有符合的和下标越界的都改为null
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "没有匹配到数据"));
            }
            if(current>idx){
                throw new IllegalArgumentException("提取索引" + current + "不能大于匹配次数" + idx);
            }*/
        }
    },
    //字符串分割
    //1：被拆分的字符串；【2：拆分的个数；outKey 由外界指定】3：分隔符
    SPLIT {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            // 分割字符不能为空
            if (DSUtil.isNull(dc0)) {
                throw new IllegalArgumentException("被拆分的字符串不能是null");
            }

           Object outputNumber =tTransformation.getInfoMap().get(TTransformationInfoType.OUTPUT_NUMBER.dbValue);
           Object delimiter = getDataCell(1,inKeyList.get(1),in,tTransformation);// tTransformation.getInfoMap().get(TTransformationInfoType.DELIMITER.dbValue);
            int splitType =(int)tTransformation.getInfoMap().get(TTransformationInfoType.SPLIT_TYPE.dbValue);
            if (outputNumber == null) {
                throw new IllegalArgumentException("输出个数不能是null");
            }
            if (delimiter == null) {
                throw new IllegalArgumentException("分隔符不能是null");
            }
            int number = Integer.valueOf(outputNumber.toString());
            String regex = (String) delimiter;
            // 分割
            if(splitType == 0) {
                int i = 0;
                for (String str : dc0.getData().toString().split(regex, number)) {
                    in.put(outKeyList.get(i++), new DataCell(TDataType.STRING, str));
                }
            // 匹配
            } else if(splitType == 1) {
                Pattern pattern = null;
                try{
                    pattern = Pattern.compile(regex);
                } catch (Exception e){
                    throw new IllegalArgumentException("分隔符不合法");
                }
                Matcher matcher = pattern.matcher(dc0.getData().toString());

                if(matcher.find()) {
                    int count = matcher.groupCount();
                    count = count>number ?number:count;
                    for(int i=0; i<count; i++) {
                        in.put(outKeyList.get(i), new DataCell(TDataType.STRING, matcher.group(i+1)));
                    }
                }

            } else {
                throw new RuntimeException("SPLIT 不存在该分割类型：" + splitType);
            }

        }
    },
    //取ASCII代码
    //返回一个字符的ASCII数值
    //in:VARCHAR out:INTEGER
    ASCII {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.INT, 0);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null || dc0.getData().toString().length()==0) {
             //   throw new IllegalArgumentException("输入数据不能是null");
                result.setData(null);
                in.put(outKeyList.get(0), result);
                return;
            }
            result.setData((int) dc0.getData().toString().charAt(0));
            in.put(outKeyList.get(0), result);
        }
    },
    //返回一个字符的Unicode数值
    //in:VARCHAR out:INTEGER
    UNICODE {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            ASCII.transferring(in, tTransformation);
        }
    },
    //计算两个字符串的相似度。
    //in:VARCHAR out:DOUBLE
    SIMILARITY {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.DOUBLE, 0.0);
            if (inKeyList.size() != 2) {
                in.put(ConstantsUtil.ERROR_KEY, new DataCell(TDataType.STRING,"参数不是2个"));
                return;
            }
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            DataCell dc1 = getDataCell(1,inKeyList.get(1),in,tTransformation);
            if (dc0 != null && dc0.getData() != null && dc1 != null && dc1.getData() != null) {
                double similarity = 0;
               int algoidx= (Integer) tTransformation.getInfoMap().get(TTransformationInfoType.ALGORITHM.dbValue.toLowerCase());
                switch (algoidx) {  //界面上只可选择 BASE， SOUNDEX
                    case 0:
                        similarity = SimilarityCalculation.BASE.getResult(dc0.getData().toString(), dc1.getData().toString());
                        break;
                    case 1:
                        similarity = SimilarityCalculation.SOUNDEX.getResult(dc0.getData().toString(), dc1.getData().toString());
                        break;
                    case 2:  //GST 方法没有实现
                        similarity = SimilarityCalculation.GST.getResult(dc0.getData().toString(), dc1.getData().toString());
                        break;
                    case 3 :
                        similarity = SimilarityCalculation.LCS.getResult(dc0.getData().toString(), dc1.getData().toString());
                        break;
                    default:
                        throw new IllegalArgumentException("SIMILARITY没有该类型的算法");
                }
                result.setData(similarity);
            }
            in.put(outKeyList.get(0), result);
        }
    },
    //把一个ASCII整型数值转换为一个对应的字符。
    //in:INTEGER  out:CHAR
    CHAR {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.STRING, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null ) {
              //  throw new IllegalArgumentException("CHAR参数不能是null");
                in.put(outKeyList.get(0), result);
                return;
            }else if(!NumberUtils.isNumber(dc0.getData().toString())){
                throw new IllegalArgumentException("CHAR参数不是数字");
            }
            result.setData((char) (NumberUtils.toInt(dc0.getData().toString())));
            in.put(outKeyList.get(0), result);
        }
    },
    //搜索一个字符串，返回另一个字符串（可以是正则表达式）在其中第1次出现的位置。如果没有出现返回0，下表从1开始.
    //0：源字符串 ,1：要搜索的目标字符串表达式
    PATTERNINDEX {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.INT, -1);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation); // 源字符串
            DataCell dc1 = getDataCell(1,inKeyList.get(1),in,tTransformation); // 目标字符串

            //
            if (DSUtil.isNull(dc1) ) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"要搜索的目标字符串不能是null"));
                return;
            }
            /*if(StringUtils.isWhitespace(dc1.getData().toString())){ // 多个空格的长度是0， 测试要求：与SQLserver保持一致
                in.put(outKeyList.get(0), result);
                return;
            }*/
            if (DSUtil.isNull(dc0)) {
                result.setData(null);
                in.put(outKeyList.get(0), result);
                return;
            }
            String data = dc0.getData().toString();//源字符串
            String reg = dc1.getData().toString();//目标字符串
            int idx = data.indexOf(reg);
            if(idx == -1){ //目标字符串可以是正则表达式，用正则匹配一次
                try {
                    //先校验一遍，是否是符合正则，不符合，规范化正则
                    Matcher matcher = Pattern.compile(reg).matcher(data);
                    if (matcher.find()) {
                        // result.setData(matcher.group(0));//第一次匹配的值
                        int st = matcher.start();// 第一次匹配的位置(没有匹配)
                        result.setData(st);
                    }
                } catch (Exception e){
                    //reg = com.eurlanda.datashire.engine.util.StringUtils.formatPattern(reg);
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING,"输入的匹配规则不合法"));
                    return;
                }
            }else{
                result.setData(idx);
            }
            in.put(outKeyList.get(0), result);
        }
    },
    //重复一个字符串指定次数
    //inKey：被重复的字符串
    //info：重复次数
    REPLICATE {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.STRING, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            DataCell dc1 = getDataCell(1,inKeyList.get(1),in,tTransformation);
            if ( ! DSUtil.isNull(dc1) && ! isInteger(dc1.getData().toString())) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "重复次数数不是整数"));
               return;
            }
            if( DSUtil.isNull(dc0) || DSUtil.isNull(dc1)){
                in.put(outKeyList.get(0), result);
                return;
            }
            try {
                Object repeatCount = dc1.getData();
                int count = NumberUtils.toInt(repeatCount.toString());
                if (count >= 0) {
                    StringBuilder builder = new StringBuilder();
                    while (count-- > 0) {
                        builder.append(dc0.getData().toString());
                    }
                    result.setData(builder.toString());
                } else {
                    result.setData(null);
                }
                in.put(outKeyList.get(0), result);
            } catch (Exception e){
                e.printStackTrace();
            } catch (Throwable e){
                if(e instanceof OutOfMemoryError){
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING, "重复次数超过空间大小"));
                    return;
                }
            }
        }
    },
    NUMERICTOSTRING {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.STRING, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null ||
                    !NumberUtils.isNumber(dc0.getData().toString())) {
//                throw new TransformationException(in, tTransformation, "transform data is illegal!");
                result.setData("数值转化成字符异常，该字段不能是空");
                in.put(ERROR_KEY, result);
            } else {
                result.setData(dc0.getData().toString());
                in.put(outKeyList.get(0), result);
            }
        }
    },
    STRINGTONUMERIC {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();

            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if(! DSUtil.isNull(dc0) && !isNumber(dc0.getData().toString())){
                DataCell result = new DataCell(TDataType.STRING, null);
                result.setData("字符转化成数值异常，该字段值不能转换为数字：[" + dc0.getData().toString() + "]");
                in.put(ERROR_KEY, result);
                return;
            }
            if (DSUtil.isNull(dc0)) {
                DataCell result = new DataCell(TDataType.STRING, null);
              //  result.setData("字符转化成数值异常，该字段不能是null");
                in.put(outKeyList.get(0), result);
                return;
            }  else if(StringUtils.isWhitespace(dc0.getData().toString()) ){
                DataCell result = new DataCell(TDataType.STRING, 0);
               // result.setData("字符转化成数值异常，该字段不能是"+dc0.getData().toString());
                in.put(outKeyList.get(0), result);
                return;
            } else {
                DataCell result = new DataCell(TDataType.DOUBLE, null);
                result.setData(Double.valueOf(dc0.getData().toString()));
                in.put(outKeyList.get(0), result);
            }
        }
    },
    //在一个字符串中寻找一个子串，并全部替换为另外一个字串
    //info needs list (first:被替换字符串 second:替换字符串)
    REPLACE {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.STRING, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            DataCell dc1 = getDataCell(1,inKeyList.get(1),in,tTransformation);
            DataCell dc2 = getDataCell(2,inKeyList.get(2),in,tTransformation);
            if(DSUtil.isNull(dc0) || DSUtil.isNull(dc1) || DSUtil.isNull(dc2) ){
                in.put(outKeyList.get(0), result);
                return;
            }
            String source = (String) dc0.getData();
            String replacement = (String) dc1.getData();
            String reg = dc2.getData().toString();
            //规范化正则表达式(特殊字符进行转义)
            /*if(StringUtils.isNotEmpty(reg)) {
                reg = com.eurlanda.datashire.engine.util.StringUtils.formatPattern(reg);
            }*/
            if(StringUtils.isEmpty(reg)){
                result.setData(source);
                in.put(outKeyList.get(0),result);
                return;
            }
            source = source.replaceAll(reg,replacement);
            result.setData(source);
            in.put(outKeyList.get(0), result);
        }
    },
    //从一个字符串的左边中截取特定长度的字串。
    //1：被截取的源字符串；2：截取的长度
    LEFT {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.STRING, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            DataCell dc1 = getDataCell(1,inKeyList.get(1),in,tTransformation);
            if( ! DSUtil.isNull(dc1) && !isNumber(dc1.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"截取长度不是数字"));
                return;
            }
            if( ! DSUtil.isNull(dc1) && NumberUtils.toInt(dc1.getData().toString()) < 0){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"截取长度不能是负数"));
                return;
            }
            if (DSUtil.isNull(dc0) ||  DSUtil.isNull(dc1)) {
                in.put(outKeyList.get(0), result);
                return;
            }
            String sourcestr = dc0.getData().toString();
            int cufoffcount = NumberUtils.toInt(dc1.getData().toString());
            if (sourcestr.length() >= cufoffcount){ //字符串长度大于截取长度
                result.setData(sourcestr.substring(0, cufoffcount));
            } else {
                result.setData(sourcestr);
            }
            in.put(outKeyList.get(0), result);
        }
    },
    //从一个字符串的右边中截取特定长度的字串。
    RIGHT {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.STRING, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            DataCell dc1 = getDataCell(1,inKeyList.get(1),in,tTransformation);
            if(! DSUtil.isNull(dc1) && !isNumber(dc1.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"RIGHT第二个参数不是数字"));
                return;
            }
            if(! DSUtil.isNull(dc1) && Integer.valueOf(dc1.getData().toString())<0){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"RIGHT第二个参数不能是负数"));
                return;
            }
            if (DSUtil.isNull(dc0) ||  DSUtil.isNull(dc1)) {
                in.put(outKeyList.get(0), result);
                return;
            }
            Object cutofflength = dc1.getData();//要截取的长度
            if (dc0.getData() == null) {
                in.put(outKeyList.get(0), result);
            }
            String sourcestr = dc0.getData().toString();
            if (sourcestr == null) {
                in.put(outKeyList.get(0), result);
            } else {
                int count = NumberUtils.toInt(cutofflength.toString());
                int max = sourcestr.length();
                if (max >= count) {
                    result.setData(sourcestr.substring(max - count, max));
                } else {
                    result.setData(sourcestr);
                }
                in.put(outKeyList.get(0), result);
            }
        }
    },
    //1：源字符串；2：开始截取的位置；3：截取的长度
    //按照sqlserver，开始截取的位置，从1开始，可以是负数
    SUBSTRING {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.STRING, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            DataCell dc1 = getDataCell(1,inKeyList.get(1),in,tTransformation);
            DataCell dc2 = getDataCell(2,inKeyList.get(2),in,tTransformation);
            if(DSUtil.isNull(dc0)){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"SUBSTRING源字符串不能是null"));
                return;
            }
            if(! DSUtil.isNull(dc1) && !isInteger(dc1.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"SUBSTRING开始截取的位置不是整数"));
                return;
            }
          /*  if(! DSUtil.isNull(dc1) && Integer.valueOf(dc1.getData().toString())<0){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"SUBSTRING开始截取的位置不能是负数"));
                return;
            } */
            if(! DSUtil.isNull(dc2) && !isInteger(dc2.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"SUBSTRING截取长度不是整数"));
                return;
            }
            if(! DSUtil.isNull(dc2) && Integer.valueOf(dc2.getData().toString())<0){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"SUBSTRING截取长度不能是负数"));
                return;
            }
            if (DSUtil.isNull(dc1) ||  DSUtil.isNull(dc2)) {
                in.put(outKeyList.get(0), result);
                return;
            }
            Object cutoffstartposition =dc1.getData();
            Object cutofflength = dc2.getData();
            String sourcestr = dc0.getData().toString();
            if (dc0 != null && StringUtils.isNotEmpty(dc0.getData().toString())
                    && isInteger(cutoffstartposition.toString())
                    && isInteger(cutofflength.toString())
                    ) {
                int strmaxlength = sourcestr.length();
                int cutoffbeginidx = NumberUtils.toInt(cutoffstartposition.toString());
                int _cutofflength = NumberUtils.toInt(cutofflength.toString());
               /* int cutoffendidx = cutoffbeginidx + _cutofflength;
                if (cutoffbeginidx >= strmaxlength) { // 当截取开始位置比字符串长度还大时，
                    result.setData("");
                } else if (cutoffendidx >= strmaxlength) { // 当截取结束位置比字符串长度还大时
                    result.setData(sourcestr.substring(cutoffbeginidx, strmaxlength));
                } else {
                    result.setData(sourcestr.substring(cutoffbeginidx, cutoffendidx));
                }*/
                result.setData(substringLikeSQLServer(sourcestr,cutoffbeginidx,_cutofflength));
            } else if (sourcestr == null) {
                result = new DataCell(TDataType.STRING, null);
            } else if (StringUtils.isBlank(sourcestr)) { //  空字符串如何处理
                result.setData("");
            }
            in.put(outKeyList.get(0), result);
        }
    },
    LENGTH {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.INT, 0);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if(DSUtil.isNull(dc0)){
                result.setData(null);
                in.put(outKeyList.get(0), result);
                return;
            }
            if(StringUtils.isWhitespace(dc0.getData().toString())){ // 多个空格的长度是0， 测试要求：与SQLserver保持一致
                in.put(outKeyList.get(0), result);
                return;
            }
            result.setData(trimEnd(dc0.getData().toString()).length()); //后面的空格不计算长度， 测试要求：与SQLserver保持一致
            in.put(outKeyList.get(0), result);
        }
    },
    REVERSE {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.STRING, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 != null && dc0.getData() != null) {
                result.setData(new StringBuilder().append(dc0.getData().toString()).reverse().toString());
            }
            in.put(outKeyList.get(0), result);
        }
    },
    LEFTTRIM {  //移除字符串左侧的所有空格
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.STRING, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 != null && dc0.getData() != null) {
                String res = trimStart(dc0.getData().toString());
                result.setData(res);
            }
            in.put(outKeyList.get(0), result);
        }
    },
    RIGHTTRIM {   //移除字符串右侧的所有空格
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.STRING, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 != null && dc0.getData() != null) {
                String sourcestr=dc0.getData().toString();
                String resstr =trimEnd(sourcestr);
                result.setData(resstr);
            }
            in.put(outKeyList.get(0), result);
        }
    },
    TRIM {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.STRING, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 != null && dc0.getData() != null) {
                String res = trimEnd(trimStart(dc0.getData().toString()));
                result.setData(res);
            }
            in.put(outKeyList.get(0), result);
        }
    },
    ROWNUMBER {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            throw new NotImplementedException();
        }
    },
    SYSTEMDATETIME {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.TIMESTAMP, TDataType.newTimestamp());
            in.put(outKeyList.get(0), result);
        }
    },
    STRINGTODATETIME {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.TIMESTAMP, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 != null && dc0.getData() != null) {
                if (StringUtils.isBlank(dc0.getData().toString())) {
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING, "要转换的字符串是空"));
                    return;
                }
                String dtstr = (String) dc0.getData();
               // result.setData(DateUtil.util2Timestamp(DateTimeUtils.parserDateTime(dtstr, FULL_TIME_FORMAT)));
                boolean isdatetimeformat=isyyyyMMDDHHmmssFormat(dtstr.trim());
                if(!isdatetimeformat ){  //日期时间格式
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING, "日期时间格式不正确，应该是 yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]"));
                    return;
                }
                if (dtstr.contains(".")) {  // 含毫秒 纳秒
                    result.setData(StringToTimestampWithNaso(dtstr.trim()));
                } else {  //不含毫秒 纳秒
                    result.setData(DateUtil.util2Timestamp(DateTimeUtils.parserDateTime(dtstr.trim(), FULL_TIME_FORMATNoNaos)));
                }
            }
            in.put(outKeyList.get(0), result);
        }
    },
    DATETIMETOSTRING {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.STRING, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 != null && dc0.getData() != null) {
                if (dc0.getData() instanceof java.sql.Timestamp) {
                    result.setData(dc0.getData().toString());
                    /*
                    if(dc0.getData().toString().contains(".")){ //带毫秒 纳秒
                        result.setData(dc0.getData().toString());
                    }else { //不带毫秒 纳秒
                        result.setData(DateFormatUtils.format((Date) dc0.getData(), FULL_TIME_FORMAT));
                    }  */
                }else if(dc0.getData() instanceof String) {
                    result.setData(dc0.getData().toString());
                }
                else {
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING, "transform data is illegal! " + in));
                    return;
                }
            }
            in.put(outKeyList.get(0), result);
        }
    },
    YEAR {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.INT, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null) {
                //throw new IllegalArgumentException("YEAR参数不能是null或不是Date类型");
                in.put(outKeyList.get(0), result);
                return;
            }
            if ( !(dc0.getData() instanceof Date)) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "YEAR参数不是Date类型 "));
                return;
            }else if(dc0.getData() == null){
                in.put(outKeyList.get(0), result);
                return;
            }
            Calendar now  = Calendar.getInstance();
            now.setTime((Date) dc0.getData());
            result.setData(now.get(Calendar.YEAR));
            in.put(outKeyList.get(0), result);
        }
    },
    MONTH {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.INT, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (inKeyList.size() != 1 || dc0 == null || dc0.getData() == null || !(dc0.getData() instanceof Date)) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "MONTH不能是null或不是Date类型"));
                return;
            }
            if (dc0 == null || dc0.getData() == null || dc0.getData() == null) {
                in.put(outKeyList.get(0), result);
                return;
            }
            Calendar now  = Calendar.getInstance();
            now.setTime((Date) dc0.getData());
            result.setData(now.get(Calendar.MONTH) + 1);
            in.put(outKeyList.get(0), result);
        }
    },
    DAY {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.INT, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null ) {
               // throw new IllegalArgumentException("DAY不能是null或不是Date类型");
                in.put(outKeyList.get(0), result);
                return;
            }
            if (!(dc0.getData() instanceof Date)) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "DAY参数不是Date类型"));
                return;
            }else if( dc0.getData() == null ){
                in.put(outKeyList.get(0), result);
                return;
            }
            Calendar now  = Calendar.getInstance();
            now.setTime((Date) dc0.getData());
            result.setData(now.get(Calendar.DAY_OF_MONTH));
            in.put(outKeyList.get(0), result);
        }
    },
    DATEDIFF {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.INT, 0);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            DataCell dc1 = getDataCell(1,inKeyList.get(1),in,tTransformation);
            Object differenceType = tTransformation.getInfoMap().get(TTransformationInfoType.DIFFERENCE_TYPE.dbValue);
            if(! DSUtil.isNull(dc0) && ! (dc0.getData() instanceof Timestamp)){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, dc0.getData()+"不是日期类型"));
                return;
            }
            if(! DSUtil.isNull(dc1) && ! (dc1.getData() instanceof Timestamp)){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, dc1.getData()+"不是日期类型"));
                return;
            }
            if(differenceType == null){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "differenceType不能是null"));
                return;
            }
           if(DSUtil.isNull(dc0) || DSUtil.isNull(dc1) ){
               result.setData(null);
               in.put(outKeyList.get(0), result);
               return;
           }
            //默认接受java.utils.date
           /* result.setData(DifferenceType.getTimeDateDiff2(((Date) dc0.getData()).getTime(),
                    ((Date) dc1.getData()).getTime(),
                    DifferenceType.parse((int) differenceType)));*/
            result.setData(DifferenceType.getTimeDateDiff((Timestamp) dc0.getData(),
                          (Timestamp) dc1.getData(), DifferenceType.parse((int) differenceType)));
            in.put(outKeyList.get(0), result);
        }
    },
    FORMATDATE {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.TIMESTAMP, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            Object dateFormat =  tTransformation.getInfoMap().get(TTransformationInfoType.DATE_FORMAT.dbValue);
            if (dateFormat != null &&  StringUtils.isBlank(dateFormat.toString())) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "FORMATDATE 异常，日期格式不能是空"));
                return;
            }
            if (DSUtil.isNull(dc0) || dateFormat==null) {
                in.put(outKeyList.get(0), result);
                return;
            }
            try {
                result.setData(DateUtil.util2Timestamp(DateUtil.parse(dateFormat.toString(), dc0.getData().toString())));
              //  result.setData(formatDateTime(dc0.getData().toString(),dateFormat.toString()));
            } catch (Exception e) {  //ParseException
                throw new IllegalArgumentException("不能按格式"+dateFormat.toString()+"格式化"+dc0.getData().toString());
            }
            in.put(outKeyList.get(0), result);
        }
    },
    ABS {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            DataCell result = new DataCell(dc0.getdType(), null);
            if (dc0 == null || dc0.getData() == null) {
              // in.put(ERROR_KEY, new DataCell(TDataType.STRING, "ABS的参数不能是null"));
                in.put(outKeyList.get(0), result);
                return ;
            } else if(!NumberUtils.isNumber(dc0.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"ABS参数不是数字"));
                return ;
            }
            //DataCell result = new DataCell(dc0.getdType(), null);
            try {
                if (dc0.getData() instanceof Long) {
                    result.setData(Math.abs((long) dc0.getData()));
                } else if (dc0.getData() instanceof Integer) {
                    result.setData(Math.abs((int) dc0.getData()));
                } else if (dc0.getData() instanceof Double) {
                    result.setData(Math.abs((double) dc0.getData()));
                } else if (dc0.getData() instanceof Float) {
                    result.setData(Math.abs((float) dc0.getData()));
                } else if (dc0.getdType()== TDataType.BIG_DECIMAL) {
                    result.setData(new BigDecimal(dc0.getData().toString()).abs());
                } else if(dc0.getdType() == TDataType.TINYINT || dc0.getdType() == TDataType.SHORT){
                    Byte b = (Byte)dc0.getData();
                    int v = b.intValue();
                    Integer rv = Math.abs(v);
                    result.setData(rv.byteValue());
                }
            } catch (Exception e) {
                throw new TransformationException(in, tTransformation, e);
            }
            in.put(outKeyList.get(0), result);
        }
    },
    RANDOM {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.DOUBLE, random.nextDouble());
            in.put(outKeyList.get(0), result);
        }
    },
    ACOS {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.DOUBLE, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (DSUtil.isNull(dc0)) {
               // throw new IllegalArgumentException("弧度值不能是null");
                in.put(outKeyList.get(0), result);
                return;
            }else if(!NumberUtils.isNumber(dc0.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "ACOS参数不是数字"));
                return;
            }
            double d = Double.valueOf(dc0.getData().toString());
            if(d<-1 || d >1) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "ACOS入参值不在[-1，1]区间"));
                return;
            }
            try {
                result.setData(Math.acos(d));//返回值是弧度
            } catch (Exception e) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, e.getMessage()));
                return;
            }
            in.put(outKeyList.get(0), result);
        }
    },
    EXP {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.DOUBLE, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null ) {
               // throw new IllegalArgumentException("指数不能是null");
                in.put(outKeyList.get(0), result);
                return;
            }else if(!NumberUtils.isNumber(dc0.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "EXP参数不是数字"));
                return;
            }
            try {
                Double v = Math.exp(Double.valueOf(dc0.getData().toString()));
                if(v.isNaN() || v.isInfinite()){
                    //计算后的数值是正无穷或负无穷或空
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING, "计算结果超出边界"));
                } else {
                    result.setData(Math.exp(Double.valueOf(dc0.getData().toString())));
                }
            } catch (Exception e) {
                throw new TransformationException(in, tTransformation, e);
            }
            in.put(outKeyList.get(0), result);
        }
    },
    ROUND {
        /**
         * 四舍五入 保留小数
         *需要保留小数的double值
         * @param value
         * @param num   需要保留的位数
         * @return double
         */
        public double round(Double value, int num) {
            if (value == null) {
                return 0d;
            }
            BigDecimal bigDecimal = new BigDecimal(value);
            value = bigDecimal.setScale(num, BigDecimal.ROUND_HALF_UP).doubleValue();
            return value;
        }

        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.DOUBLE, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            DataCell dc1 = getDataCell(1,inKeyList.get(1),in,tTransformation);
            if(! DSUtil.isNull(dc0) && !isNumber(dc0.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"ROUND第一个参数是数字"));
            }
            if(! DSUtil.isNull(dc1) && !isNumber(dc1.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"ROUND第二个参数不是数字"));
            }
            if (DSUtil.isNull(dc0) ||  DSUtil.isNull(dc1)) {
                in.put(outKeyList.get(0), result);
                return;
            }
            int num = (int) getDataCell(1,inKeyList.get(1),in,tTransformation).getData();
            if(!NumberUtils.isNumber(dc0.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "ROUND参数不是数字"));
                return;
            }
            try {
                result.setData(round(Double.valueOf(dc0.getData().toString()), num));
            } catch (Exception e) {
                throw new TransformationException(in, tTransformation, e);
            }
            in.put(outKeyList.get(0), result);
        }
    },
    ASIN {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.DOUBLE, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null ) {
              //  throw new IllegalArgumentException( "ASIN 的参数不能是null");
                in.put(outKeyList.get(0), result);
                return;
            }else if(!NumberUtils.isNumber(dc0.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "ASIN参数不是数字"));
                return;
            }
            double d = Double.valueOf(dc0.getData().toString());
            if(d<-1 || d >1) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "asin值不在[-1，1]区间"));
                return;
            }
            try {
                result.setData(Math.asin(d));//输入值是弧度
            } catch (Exception e) {
              //  throw new TransformationException(in, tTransformation, e);
                throw e;
            }
            in.put(outKeyList.get(0), result);
        }
    },
    FLOOR {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.DOUBLE, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null ) {
               // throw new IllegalArgumentException("FLOOR的参数不能是null");
                in.put(outKeyList.get(0), result);
                return;
            }else if(!NumberUtils.isNumber(dc0.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "FLOOR参数不是数字"));
                return;
            }
            try {
                result.setData(Math.floor(Double.valueOf(dc0.getData().toString())));
            } catch (Exception e) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, e.getMessage()));
             }
            in.put(outKeyList.get(0), result);
        }
    },
    SIGN {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.DOUBLE, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null ) {
                //throw new IllegalArgumentException("SIGN参数不能是null");
                in.put(outKeyList.get(0), result);
                return;
            }else if(!NumberUtils.isNumber(dc0.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "SIGN参数不是数字"));
                return;
            }
            try {
                double val = (Double.valueOf(dc0.getData().toString()));
                result.setData(val > 0 ? 1d : (val == 0 ? 0d : -1d));
            } catch (Exception e) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, e.getMessage()));
            }
            in.put(outKeyList.get(0), result);
        }
    },
    ATAN {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.DOUBLE, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null ) {
              //  throw new IllegalArgumentException("ATAN参数不能是null");
                in.put(outKeyList.get(0), result);
                return;
            }else if(!NumberUtils.isNumber(dc0.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "ATAN的参数不是数字"));
                return;
            }
            try {
               // result.setData(Math.toDegrees(Math.atan(Double.valueOf(dc0.getData().toString()))));
                result.setData(Math.atan(Double.valueOf(dc0.getData().toString()))); //返回值是弧度
            } catch (Exception e) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, e.getMessage()));
                return;
            }
            in.put(outKeyList.get(0), result);
        }
    },
    LOG { //LOG 以e为低
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.DOUBLE, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null) {
                //throw new IllegalArgumentException( "Ln的参数不能是null");
                in.put(outKeyList.get(0), result);
                return;
            }else if( !NumberUtils.isNumber(dc0.getData().toString())) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "Ln的参数不是数字"));
                return;
            }
            try {
                double x=Double.valueOf(dc0.getData().toString());
                if(x<=0){
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING, "Ln的参数不能是0或负数"));
                    return;
                }
                result.setData(Math.log(x));
            } catch (Exception e) {
                 in.put(ERROR_KEY, new DataCell(TDataType.STRING, e.getMessage()));
            }
            in.put(outKeyList.get(0), result);
        }
    },
    SIN {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.DOUBLE, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null ) {
               // throw new IllegalArgumentException("SIN参数不能是null");
                in.put(outKeyList.get(0), result);
                return;
            }else if(!NumberUtils.isNumber(dc0.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "SIN的参数不是数字"));
                return;
            }
            try {
               result.setData(Math.sin(Double.valueOf(dc0.getData().toString())));//输入值是弧度
            } catch (Exception e) {
                throw e;
            }
            in.put(outKeyList.get(0), result);
        }
    },
    LOG10 {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.DOUBLE, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null) {
                //throw new IllegalArgumentException("LOG10的参数不能是null");
                in.put(outKeyList.get(0), result);
                return;
            }else if( !NumberUtils.isNumber(dc0.getData().toString())) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "LOG10的参数不是数字"));
                return;
            }
            try {
                double x = Double.valueOf(dc0.getData().toString());
                if (x <= 0) {
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING, "LOG10的参数不能是0或负数"));
                    return;
                }
                result.setData(Math.log10(x));
            } catch (Exception e) {
                throw e;//new TransformationException(in, tTransformation, e);
            }
            in.put(outKeyList.get(0), result);
        }
    },
    SQRT {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.DOUBLE, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null ) {
                //throw new IllegalArgumentException("SQRT参数不能是null");
                in.put(outKeyList.get(0), result);
                return;
            }else if(!NumberUtils.isNumber(dc0.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "SQRT的参数不是数字"));
                return;
            }
            try {
                double arg=Double.valueOf(dc0.getData().toString());
                if(arg<0.0){
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING, "SQRT参数不能是负数"));
                    return;
                }
                result.setData(Math.sqrt(arg));
            } catch (Exception e) {
                throw e;
            }
            in.put(outKeyList.get(0), result);
        }
    },
    CEILING {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.LONG, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null ) {
               // throw new IllegalArgumentException("CEILING参数不能是null");
                in.put(outKeyList.get(0), result);
                return;
            }else if(!NumberUtils.isNumber(dc0.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "CEILING的参数不是数字"));
                return;
            }
            try {
                BigDecimal ceilV = new BigDecimal(Math.ceil(Double.valueOf(dc0.getData().toString())));
                ceilV.setScale(0);
                result.setData(ceilV.longValue());
            } catch (Exception e) {
                throw new TransformationException(in, tTransformation, e);
            }
            in.put(outKeyList.get(0), result);
        }
    },
    PI {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.DOUBLE, Math.PI);
            in.put(outKeyList.get(0), result);
        }
    },
    SQUARE {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.DOUBLE, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null ) {
               // throw new IllegalArgumentException("SQUARE参数不能是null");
                in.put(outKeyList.get(0), result);
                return;
            }else if(!NumberUtils.isNumber(dc0.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "SQUARE的参数不是数字"));
                return;
            }
            try {
                double val = (Double.valueOf(dc0.getData().toString()));
                result.setData(val * val);
            } catch (Exception e) {
                throw new TransformationException(in, tTransformation, e);
            }
            in.put(outKeyList.get(0), result);
        }
    },
    COS {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.DOUBLE, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null) {
               // throw new IllegalArgumentException("COS参数不能是null");
                in.put(outKeyList.get(0), result);
                return;
            }else if(!NumberUtils.isNumber(dc0.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "COS的参数不是数字"));
                return;
            }
            try {
                result.setData(Math.cos(Double.valueOf(dc0.getData().toString())));//弧度

            } catch (Exception e) {
                throw e ; //new TransformationException(in, tTransformation, e);
            }
            in.put(outKeyList.get(0), result);
        }
    },
    POWER {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.DOUBLE, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            DataCell dc1 = getDataCell(1,inKeyList.get(1),in,tTransformation);
            if(! DSUtil.isNull(dc0) && !isNumber(dc0.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"POWER的底数不是数字"));
                return;
            }
            if(! DSUtil.isNull(dc1) && !isNumber(dc1.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"POWER的指数不是数字"));
                return;
            }
            if (DSUtil.isNull(dc0) ||  DSUtil.isNull(dc1)) {
                in.put(outKeyList.get(0), result);
                return;
            }
            try {
                double a = Double.valueOf(dc0.getData().toString());
                double b = Double.parseDouble(getDataCell(1,inKeyList.get(1),in,tTransformation).getData().toString()); //Double.parseDouble(tTransformation.getInfoMap().get("power").toString());
               if (Math.abs(a) < tolerance && b<0) { // a=0,b <0
                   in.put(ERROR_KEY, new DataCell(TDataType.STRING, "POWER的底数不能是0，指数不能是负数"));
                   return;
                }
                Double v = Math.pow(a, b);
                if(v.isNaN() || v.isInfinite()){
                    //计算后的数值是正无穷或负无穷或空
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING, "计算结果超出边界"));
                    return;
                } else {
                    result.setData(Math.pow(a, b));
                }
                //result.setData(Math.pow(a, b));
            } catch (Exception e) {
                throw e;//new TransformationException(in, tTransformation, e);
            }
            in.put(outKeyList.get(0), result);
        }
    },
    TAN {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.DOUBLE, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null) {
              //  throw new IllegalArgumentException("TAN参数不能是null");
                in.put(outKeyList.get(0), result);
                return;
            }else if(!NumberUtils.isNumber(dc0.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "TAN的参数不是数字"));
                return;
            }
            try {
                result.setData(Math.tan(Double.valueOf(dc0.getData().toString())));//输入值弧度
            } catch (Exception e) {
                throw e;
            }
            in.put(outKeyList.get(0), result);
        }
    },
    COT {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.DOUBLE, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null ) {
               // throw new IllegalArgumentException("COT参数不能是null");
                in.put(outKeyList.get(0), result);
                return;
            }else if(!NumberUtils.isNumber(dc0.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "COT的参数不是数字"));
                return;
            }
            try {
                double value = Double.valueOf(dc0.getData().toString());
                if(value==0|| value % Math.PI==0){
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING, "三角余切入参不可为0、π、π的倍数"));
                    return ;
                }
                result.setData(1/Math.tan(value)); //输入值弧度
            } catch (Exception e) {
                throw e;//TransformationException(in, tTransformation, e);
            }
            in.put(outKeyList.get(0), result);
        }
    },
    RADIANS {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.DOUBLE, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null) {
               // throw new IllegalArgumentException("RADIANS参数不能是null");
                in.put(outKeyList.get(0), result);
                return;
            }else if(!NumberUtils.isNumber(dc0.getData().toString())){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "RADIANS的参数不是数字"));
                return;
            }
            try {
               result.setData(Math.toRadians(Double.valueOf(dc0.getData().toString())));
            } catch (Exception e) {
                throw e;// new TransformationException(in, tTransformation, e);
            }
            in.put(outKeyList.get(0), result);
        }
    },
    CALCULATION {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.BIG_DECIMAL, 0);
            Integer operator = (Integer)tTransformation.getInfoMap().get("operator");
            if (inKeyList.size() != 2) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "CALCULATION参数不是2个"));
                return;
            }
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            DataCell dc1 = getDataCell(1,inKeyList.get(1),in,tTransformation);

            if(DSUtil.isNull(dc0) || DSUtil.isNull(dc1)) {
                result.setData(null);
                in.put(outKeyList.get(0), result);
                return;
            } else {
                BigDecimal val1 = new BigDecimal(dc0.getData().toString());
                BigDecimal val2 = new BigDecimal(dc1.getData().toString());
                switch (operator) {
                case 0:
                    result.setData(val1.add(val2));
                    break;
                case 1:
                    result.setData(val1.subtract(val2));
                    break;
                case 2:
                    result.setData(val1.multiply(val2));
                    break;
                case 3:
                    if (val2.floatValue() == 0) {
                        in.put(ERROR_KEY, new DataCell(TDataType.STRING, "除数值不能是0"));
                        return;
                    }
                    result.setData(val1.divide(val2, 20, RoundingMode.HALF_UP));
                    break;
                }
            }

            in.put(outKeyList.get(0), result);
        }
    },
    MOD {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.BIG_DECIMAL, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            DataCell dc1 = getDataCell(1,inKeyList.get(1),in,tTransformation);
            if (!DSUtil.isNull(dc0) && !isNumber(dc0.getData().toString())) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "MOD的第一个参数不是数字"));
                return;
            }
            if (!DSUtil.isNull(dc1) && !isNumber(dc1.getData().toString())) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "MOD的第二个参数不是数字"));
                return;
            }
            if (Integer.valueOf(dc1.getData().toString()) == 0) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "除数值不能是0"));
                return;
            }
            if (DSUtil.isNull(dc0) || DSUtil.isNull(dc1)) {
                in.put(outKeyList.get(0), result);
                return;
            }
            if (Integer.valueOf(dc0.getData().toString()) == 0 ) {
                result.setData(0);
                in.put(outKeyList.get(0), result);
                return;
            }
            BigDecimal bd2 = new BigDecimal(getDataCell(1,inKeyList.get(1),in,tTransformation).getData().toString());
            BigDecimal bd = new BigDecimal(dc0.getData().toString());
            result.setData(bd.remainder(bd2));
            in.put(outKeyList.get(0), result);
        }
    },
    PROJECTID {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            Object id = tTransformation.getInfoMap().get(TTransformationInfoType.PROJECT_ID.dbValue);
            DataCell result = new DataCell(TDataType.INT, Integer.valueOf(id.toString()));
            in.put(outKeyList.get(0), result);
        }
    },
    PROJECTNAME {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            Object name = tTransformation.getInfoMap().get(TTransformationInfoType.PROJECT_NAME.dbValue);
            DataCell result = new DataCell(TDataType.STRING, name);
            in.put(outKeyList.get(0), result);
        }
    },
    SQUIDFLOWID {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            Object id = tTransformation.getInfoMap().get(TTransformationInfoType.SQUID_FLOW_ID.dbValue);
            DataCell result = new DataCell(TDataType.INT, Integer.valueOf(id.toString()));
            in.put(outKeyList.get(0), result);
        }
    },
    SQUIDFLOWNAME {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            Object name = tTransformation.getInfoMap().get(TTransformationInfoType.SQUID_FLOW_NAME.dbValue);
            DataCell result = new DataCell(TDataType.STRING, name);
            in.put(outKeyList.get(0), result);
        }
    },
    TASKID{
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            Map<String, Object> infoMap = tTransformation.getInfoMap();
            String str = (String)infoMap.get(TTransformationInfoType.TASK_ID.dbValue);
            DataCell resultDC= new DataCell(TDataType.STRING, str);
            in.put(outKeyList.get(0),resultDC);
        }
    },
    JOBID {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            Object id = tTransformation.getInfoMap().get(TTransformationInfoType.JOB_ID.dbValue);
            DataCell result = new DataCell(TDataType.INT, Integer.valueOf(id.toString()));
            in.put(outKeyList.get(0), result);
        }
    },
    /**
     * todo 不知道是否添加
     // 分词，生成特征值
     SENTIMENT {
    @SuppressWarnings("unchecked")
    public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
    List<Integer> inKeyList = tTransformation.getInKeyList();
    List<Integer> outKeyList = tTransformation.getOutKeyList();
    Object dict = tTransformation.getInfoMap().get(TTransformationInfoType.DICT.dbValue);
    DataCell result = new DataCell(TDataType.LABELED_POINT, null);
    if (dict == null || inKeyList.size() != 2) {
    throw new TransformationException(in, tTransformation, " transform data is illegal!");
    }
    DataCell dc0 = in.get(inKeyList.get(0));
    DataCell dc1 = in.get(inKeyList.get(1));
    double[] features = TFeaturesTransformationUtils.getEmotionFeature((String) dc1.getData(), (ArrayList<String>) dict);
    LabeledPoint lp = new LabeledPoint((double) dc0.getData(), Vectors.dense(features));
    result.setData(lp);
    in.put(outKeyList.get(0), result);
    }
    },*/
    // 根据模型预测
    PREDICT {
        @SuppressWarnings("unchecked")
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeys = tTransformation.getInKeyList();
            DataCell inPara = getDataCell(0,tTransformation.getInKeyList().get(0),in,tTransformation);
            Integer outKey = tTransformation.getOutKeyList().get(0);
            DataCell result = null;

            Map<String, Object> infoMap = tTransformation.getInfoMap();
            // 模型类型
            SquidTypeEnum squidTypeEnum = (SquidTypeEnum)infoMap.get(TTransformationInfoType.PREDICT_MODEL_TYPE.dbValue);
            // todo 量化对于空值的处理
            if(squidTypeEnum != SquidTypeEnum.QUANTIFY) {
                if (inPara == null || inPara.getData() == null) {
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING, "预测数据值是null"));
                    return;
                }
            } else {
                if (inPara == null || inPara.getData() == null) {
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING, "预测数据是null"));
                    return;
                }
            }
            Object modelObj = getModleObject(in, inKeys, infoMap, squidTypeEnum,TTransformationInfoType.PREDICT_DM_MODEL.dbValue);
            result = DSUtil.predict(modelObj, squidTypeEnum, inPara);

            /**
            switch (squidTypeEnum) {
                case LOGREG:
                    LogisticRegressionModel lgModel = genModel(modelObj, LogisticRegressionModel.class);
                    result = new DataCell(TDataType.DOUBLE,lgModel.predict(TrainDataConvertor.convertDoubleVector(inPara.getData().toString())));
                    break;
                case LINEREG:
                    LinearRegressionModel lrModel = genModel(modelObj, LinearRegressionModel.class);
                    result = new DataCell(TDataType.DOUBLE,lrModel.predict(TrainDataConvertor.convertDoubleVector(inPara.getData().toString())));
                    break;
                case RIDGEREG:
                    RidgeRegressionModel rrModel = genModel(modelObj, RidgeRegressionModel.class);
                    result = new DataCell(TDataType.DOUBLE,rrModel.predict(TrainDataConvertor.convertDoubleVector(inPara.getData().toString())));
                    break;
                case SVM:
                    SVMModel svmModel = genModel(modelObj, SVMModel.class);
                    result = new DataCell(TDataType.DOUBLE,svmModel.predict(TrainDataConvertor.convertDoubleVector(inPara.getData().toString())));
                    break;
                case NAIVEBAYES:    // 朴素贝叶斯
                    NaiveBayesModel nbModel = genModel(modelObj, NaiveBayesModel.class);
                    result = new DataCell(TDataType.DOUBLE,nbModel.predict(TrainDataConvertor.convertDoubleVector(inPara.getData().toString())));
                    break;
                // see ALSPredictProcessor
//                case ALS:
//                    MatrixFactorizationModel mfModel = genModel(modelObj, MatrixFactorizationModel.class);
//                    String csn = inPara.getData().toString();
//                    String[] strs = csn.split(",");
//                    // userId, ProductId
//                    result = new DataCell(TDataType.DOUBLE ,mfModel.predict(Integer.parseInt(strs[0]), Integer.parseInt(strs[1])));
//                    break;
                case KMEANS:
                    KMeansModel kmModel = genModel(modelObj, KMeansModel.class);
                    result = new DataCell(TDataType.INT ,kmModel.predict(TrainDataConvertor.convertDoubleVector(inPara.getData().toString())));
                    break;
                case QUANTIFY:
                    QuantifyModel qModel = genModel(modelObj, QuantifyModel.class);
                    result = new DataCell(TDataType.DOUBLE, qModel.predict(inPara.getData()));
                    break;
                case DISCRETIZE:
                    DiscretizeModel dModel = genModel(modelObj, DiscretizeModel.class);
                    result = new DataCell(TDataType.DOUBLE, dModel.predict(ClassUtil.convert2Double(inPara.getData())));
                    break;
                case DECISIONTREE:
                    DecisionTreeModel dtModel = genModel(modelObj, DecisionTreeModel.class);
                    result = new DataCell(TDataType.DOUBLE, dtModel.predict(TrainDataConvertor.convertDoubleVector(inPara.getData().toString())));
                    break;
                default:
                    throw new RuntimeException("DM 模型不匹配，" + squidTypeEnum);
            }
            */
            in.put(outKey, result);

            /**
             Object dict = tTransformation.getInfoMap().get(TTransformationInfoType.DICT.dbValue);
             Object info = tTransformation.getInfoMap().get(TTransformationInfoType.NAIVE_BAYES_MODEL.dbValue);
             if (info == null || dict==null) {
             throw new TransformationException(in, tTransformation, " transform data is illegal!");
             }
             DataCell dc0 = in.get(inKeyList.get(0));
             ArrayList<String> rDict = (ArrayList<String>) dict;
             NaiveBayesModel model = (NaiveBayesModel) info;
             Double prValue = model.predict(Vectors.dense(TFeaturesTransformationUtils.getEmotionFeature((String) dc0.getData(), rDict)));
             result.setData(prValue);
             in.put(outKeyList.get(0), result);
             */

        }
    },
    TOKENIZATION {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            throw new NotImplementedException();
        }
    },
    QUANTIFY {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            throw new NotImplementedException();
        }
    },
    DISCRETIZE {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            throw new NotImplementedException();
        }
    },
    NUMASSEMBLE {//数组组装,组装结果类似 1,2,3,,
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeys = tTransformation.getInKeyList();
            StringBuilder sb = new StringBuilder();
            int idx = 0;
            for(int i=0;i<inKeys.size();i++){
                int ik = inKeys.get(i);
                if (idx == 0) {
                    idx += 1;
                } else { //有部分是null时也要加逗号
                    sb.append(",");
                }
                if (getDataCell(i,ik,in,tTransformation) != null && getDataCell(i,ik,in,tTransformation).getData() != null) {
                    sb.append(getDataCell(i,ik,in,tTransformation).getData().toString());
                }
            }
            // 获得输入列
          in.put(tTransformation.getOutKeyList().get(0), new DataCell(TDataType.CSN, sb.toString()));
        }
    },
    STRASSEMBLE {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            throw new NotImplementedException();
        }
    },
    // 组装
    ASSEMBLE {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeys = tTransformation.getInKeyList();
            StringBuilder sb = new StringBuilder();
            int idx = 0;
            for(int i=0;i<inKeys.size();i++){
                int ik = inKeys.get(i);
                if(idx == 0) {
                    idx += 1;
                } else {
                    sb.append(",");
                }
                sb.append(getDataCell(i,ik,in,tTransformation).getData().toString());
            }
            // 获得输入列
            in.put(tTransformation.getOutKeyList().get(0), new DataCell(TDataType.CSN, sb.toString()));
        }
    },
    // 将csn 字符串转化为 labledpoint
    CONVERT_LABLEDPOINT {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            // 获得输入列
            in.put(tTransformation.getOutKeyList().get(0), new DataCell(TDataType.LABELED_POINT, TrainDataConvertor.convertLabledPoint(getDataCell(0,tTransformation.getInKeyList().get(0),in,tTransformation).getData().toString())));
        }
    },
    // 将csn 字符串转化为 rating
    CONVERT_RATING {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            // 获得输入列
            in.put(tTransformation.getOutKeyList().get(0), new DataCell(TDataType.LABELED_POINT, TrainDataConvertor.convertRating(getDataCell(0,tTransformation.getInKeyList().get(0),in,tTransformation).getData().toString())));
        }
    },
    TRAIN {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            DataCell inDC = getDataCell(0,tTransformation.getInKeyList().get(0),in,tTransformation);
            if(inDC.getData() == null) {
                logger.debug("训练数据不能为空");
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"训练数据不能为空"));
                return;
            }
            String csnStr = inDC.getData().toString();
            SquidTypeEnum ste = (SquidTypeEnum)tTransformation.getInfoMap().get(TTransformationInfoType.TRAIN_MODEL_TYPE.dbValue);
            // 生成训练数据类型
            if(ste == SquidTypeEnum.ALS) {
                String[] arr = csnStr.split(",");
                if(arr.length !=3){
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING,"ALS训练组合不是3数，第一个元素是整数，第二个元素是整数，第三个元素是数字"));
                    return;
                }
                if(!isInteger(arr[0]) || !isInteger(arr[1]) || ! isNumber(arr[2])){
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING,"ALS训练组合不是3数，第一个元素是整数，第二个元素是整数，第三个元素是数字"));
                    return;
                }
                in.put(tTransformation.getOutKeyList().get(0), new DataCell(TDataType.RATING, TrainDataConvertor.convertRating(csnStr)));
            } else if(ste == SquidTypeEnum.DISCRETIZE
                    || ste == SquidTypeEnum.QUANTIFY){
                in.put(tTransformation.getOutKeyList().get(0), inDC);
            } else if(ste == SquidTypeEnum.ASSOCIATION_RULES){
                in.put(tTransformation.getOutKeyList().get(0), new DataCell(TDataType.CSV,  inDC.getData()));
            }else if(ste == SquidTypeEnum.LASSO
                    || ste == SquidTypeEnum.RANDOMFORESTCLASSIFIER
                    || ste == SquidTypeEnum.RANDOMFORESTREGRESSION
                    || ste == SquidTypeEnum.MULTILAYERPERCEPERONCLASSIFIER
                    || ste == SquidTypeEnum.NORMALIZER
                    || ste == SquidTypeEnum.LINEREG
                    || ste == SquidTypeEnum.LOGREG
                    || ste == SquidTypeEnum.DECISIONTREECLASSIFICATION
                    || ste == SquidTypeEnum.DECISIONTREEREGRESSION
                    || ste == SquidTypeEnum.RIDGEREG
                    || ste == SquidTypeEnum.NAIVEBAYES
                    || ste == SquidTypeEnum.SVM
                    || ste == SquidTypeEnum.BISECTINGKMEANSSQUID
                    || ste == SquidTypeEnum.KMEANS
                    ) {
                in.put(tTransformation.getOutKeyList().get(0), new DataCell(TDataType.CSN,  inDC.getData()));
            } else if(ste == SquidTypeEnum.PLS  ) { // pls 2个输入
                String inDC0 = getDataCell(0,tTransformation.getInKeyList().get(0),in,tTransformation).getData().toString();
                String inDC1 = getDataCell(1,tTransformation.getInKeyList().get(1),in,tTransformation).getData().toString();
                String contact = inDC0+";"+inDC1; // 第一个inputs是y,第二个inputs是x
                in.put(tTransformation.getOutKeyList().get(0), new DataCell(TDataType.CSN,  contact));
            } else {
                throw new RuntimeException("不支持此类型：" + ste);
            }

        }
    },
    //从String中找到国家
    COUNTRY {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();//输入
            List<Integer> outKeyList = tTransformation.getOutKeyList();//输出
            DataCell result = new DataCell(TDataType.STRING, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null || !(dc0.getData() instanceof String)) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "国家不能是null或不是字符串类型"));
                return;
            }
            String sourceStr = dc0.getData().toString();
            if (StringUtils.isBlank(sourceStr)) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "国家不能是空"));
                return;
            }
            List<String> resultValue = AreaUtil.findCountry(sourceStr);
            if (resultValue == null || resultValue.size() == 0) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "数据文件中没有找到" + sourceStr));
                return;
            }
            result.setData(resultValue.get(0));
            in.put(outKeyList.get(0), result);
        }
    },
    //从string中找到省份 级别是1
    PROVINCE {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();//输入
            List<Integer> outKeyList = tTransformation.getOutKeyList();//输出
            DataCell result = new DataCell(TDataType.STRING, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null || !(dc0.getData() instanceof String)) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "PROVINCE参数不能是null"));
                return;
            }
            String sourceStr=dc0.getData().toString();
            AreaProxy area = new AreaProxy(AreaUtil.getAreas());
            int level=1;
            List<String> resultValue = AreaUtil.findArea(sourceStr,area,level);
            String returnString = null;
            if(resultValue!=null&&resultValue.size()>0){
                returnString=resultValue.get(0);
            }
            if(returnString ==null) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "数据文件中没有找到"+sourceStr));
                return;
            }
            result.setData(returnString);
            in.put(outKeyList.get(0), result);
        }
    },
    //从string中找到城市 级别是2
    CITY {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();//输入
            List<Integer> outKeyList = tTransformation.getOutKeyList();//输出
            DataCell result = new DataCell(TDataType.STRING, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null || !(dc0.getData() instanceof String)) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "CITY参数不能是null"));
                return;
            }
            String sourceStr=dc0.getData().toString();
            AreaProxy area = new AreaProxy(AreaUtil.getAreas());
            int level=  isChinaMunicipality(sourceStr) ? 1:2; //直辖市是省级1级，其他城市是2级，
            List<String> resultValue = AreaUtil.findArea(sourceStr,area,level);
            String returnString = null;
            if(resultValue!=null&&resultValue.size()>0){
                returnString=resultValue.get(0);
            }
            if(returnString ==null) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "入参值不符合基本规范，数据文件中没有找到"+sourceStr));
                return;
            }
            result.setData(returnString);
            in.put(outKeyList.get(0), result);
        }
    },
    //通过String获得地区 级别是3
    DISTRICT {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();//输入
            List<Integer> outKeyList = tTransformation.getOutKeyList();//输出
            DataCell result = new DataCell(TDataType.STRING, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null || !(dc0.getData() instanceof String)) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "地区不能是null或不是String类型"));
                return;
            }
            String sourceStr=dc0.getData().toString();
            AreaProxy area = new AreaProxy(AreaUtil.getAreas());
            int level=3;
            List<String> resultValue = AreaUtil.findArea(sourceStr,area,level);
            String returnString = null;
            if(resultValue!=null&&resultValue.size()>0){
                returnString=resultValue.get(0);
            }
            if(returnString ==null) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"数据文件中没有找到"+sourceStr));
                return;
            }
            result.setData(returnString);
            in.put(outKeyList.get(0), result);
        }
    },
    //从字符串中获得街道 级别是4
    STREET {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();//输入
            List<Integer> outKeyList = tTransformation.getOutKeyList();//输出
            DataCell result = new DataCell(TDataType.STRING, null);

            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null || !(dc0.getData() instanceof String)) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"STREET不能是null"));
                return;
            }
            String sourceStr=dc0.getData().toString();
            AreaProxy area = new AreaProxy(AreaUtil.getAreas());
            int level=4;
            List<String> resultValue = AreaUtil.findArea(sourceStr,area,level);
            String returnString = null;
            if(resultValue!=null&&resultValue.size()>0){
                returnString=resultValue.get(0);
            }
            if(returnString ==null) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"数据文件中没有找到"+sourceStr));
                return;
            }
            result.setData(returnString);
            in.put(outKeyList.get(0), result);
        }
    },
    //拆分时间获得时间特定部分
    DATEPART {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();//输入
            List<Integer> outKeyList = tTransformation.getOutKeyList();//输出
            DataCell result = new DataCell(TDataType.INT, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null ) {
               // throw new IllegalArgumentException("DATEPART参数不能是null或不是时间类型");
                in.put(outKeyList.get(0), result);
                return;
            }
            if ( !(dc0.getData() instanceof Timestamp)) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"DATEPART参数不是时间类型"));
                return;
            }
            Timestamp sourceTime = (Timestamp) dc0.getData();
           if(sourceTime == null){
               in.put(outKeyList.get(0), result);
               return;
           }
            int inc_unit = Integer.parseInt(tTransformation.getInfoMap()
                    .get(TTransformationInfoType.INC_UNIT.dbValue).toString());
            IncUnitEnum part = null;
            try {
                String s =  tTransformation.getInfoMap().get(TTransformationInfoType.INC_UNIT.dbValue).toString();
                Integer i = Integer.parseInt(tTransformation.getInfoMap().get(TTransformationInfoType.INC_UNIT.dbValue).toString());
                part = IncUnitEnum.valueOf(inc_unit);
            } catch (EnumException e) {
                throw new TransformationException(in, tTransformation, e);
            }
            Integer resertValue = 0;
            switch (part) {
                case NANOSECOND:
                    resertValue = sourceTime.getNanos();
                    break;
                default:
                    resertValue = Integer.parseInt(DateUtil.format(part.get_value(), sourceTime));
            }
            result.setData(resertValue);
            in.put(outKeyList.get(0), result);
        }
    },
    //DateTime转换UnixTime
    DATETOUNIXTIME {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();//输入
            List<Integer> outKeyList = tTransformation.getOutKeyList();//输出
            DataCell result = new DataCell(TDataType.LONG, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null || !(dc0.getData() instanceof Timestamp)) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"DATETOUNIXTIME不能是null或不是Timestamp类型"));
                return;
            }
            Date date = (Date)dc0.getData();
            result.setData(date.getTime()/1000);
            in.put(outKeyList.get(0),result);
        }
    },
    //UNIX时间转DateTIme
    UNIXTIMETODATE {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();//输入
            List<Integer> outKeyList = tTransformation.getOutKeyList();//输出
            DataCell result = new DataCell(TDataType.TIMESTAMP, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (dc0 == null || dc0.getData() == null || !(dc0.getData() instanceof Long) && !( dc0.getData() instanceof Integer)) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"UNIXTIMETODATE不能是null或不是Long或Integer类型"));
                return;
            }
            Long unixTime = Long.parseLong(dc0.getData().toString());
            result.setData(new Timestamp(unixTime*1000));
            in.put(outKeyList.get(0),result);
        }
    },
    //时间相加
    DATEINC {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();//输入
            List<Integer> outKeyList = tTransformation.getOutKeyList();//输出
            DataCell result = new DataCell(TDataType.TIMESTAMP, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if ( ! DSUtil.isNull(dc0) &&!(dc0.getData() instanceof Timestamp) ) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"DATEINC的参数不能是null"));
                return;
            }
            if (dc0 == null || dc0.getData() == null ) {
                in.put(outKeyList.get(0),result);
                return;
            }
            Timestamp sourceTime = (Timestamp) dc0.getData();
            Timestamp resultTime = null;
            String addedValuestr = tTransformation.getInfoMap().get(TTransformationInfoType.CONSTANT_VALUE.dbValue).toString();
            if(addedValuestr == null || StringUtils.isWhitespace(addedValuestr)){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"DATEINC的增减值不能是空"));
                return;
            }
            if( !isInteger(addedValuestr)){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"DATEINC的增减值不是整数"));
                return;
            }
            Long addend = Long.parseLong(addedValuestr);
            int inc_unit = Integer.parseInt(tTransformation.getInfoMap().get(TTransformationInfoType.INC_UNIT.dbValue).toString());
            IncUnitEnum part=null;
            try{
                part = IncUnitEnum.valueOf(inc_unit);
            }catch (EnumException e){
                throw new TransformationException(in, tTransformation, e);
            }
            Calendar cc =null;
            if(part.get_key()!=IncUnitEnum.NANOSECOND.get_key()) {
                cc = Calendar.getInstance();
                cc.setTime(sourceTime);
            }
            Long[] yyyyMMddhhmmsshhnn=parseTimestamp(sourceTime.toString());
            // sqlserver dateadd 设置的时间 必须介于 1753-01-01 00:00:00 和 9999-12-31 23:59:59 之间
            // 此处用类Calendar 不会判断时间范围
            Long[] yearmonth3 = {0L,0L};
            if(part == IncUnitEnum.MONTH) {
                yearmonth3 = shangyushu(addend, 12L);//得到年月
            }
            Long year3 = yyyyMMddhhmmsshhnn[0];
            Long month3 =  yyyyMMddhhmmsshhnn[1] + yearmonth3[1];
            if(month3<0){
                year3 -= 1;
                month3 = 12 + month3;
            }else if(month3>12){
                year3 += 1;
                month3 = 12 - month3;
            }
            /*if(year3 + yearmonth3[0]< 1753 || year3+ yearmonth3[0]> 9999){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"Datetime类型时间年月应在1753-01-01 00:00:00 和 9999-12-31 00:00:00之间"));
                return;
            }*/
            switch (part){
                case NANOSECOND://纳秒操作

                    if (addend >= 0) {//判断是加还是减
                        if (addend > 999999999) {//如果超过了
                            long na = sourceTime.getNanos();//获取纳秒值进行保存
                            sourceTime = new Timestamp(sourceTime.getTime() + addend / 1000000000 * 1000);
                            //因为所加的纳秒值超过了999999999，即超过了一秒，所以先进行加秒计算
                            sourceTime.setNanos((int) na);//设置新的原始时间
                            addend = addend % 1000000000;//将所加的纳秒数进行调整，变为加完了秒数后剩下的值
                        }
                        Long namiaoadded = sourceTime.getNanos() + addend;//这里是已经相加过的值
                        if (namiaoadded >= 1000000000l) {//判断相加过后的值有没有超过1000000000，超过了则需要进位
                            resultTime = new Timestamp(sourceTime.getTime() + 1000);//毫秒
                            resultTime.setNanos(namiaoadded.intValue() - 1000000000);//纳秒
                        } else if (namiaoadded < 1000000000l && namiaoadded >= 0) {
                            resultTime = new Timestamp(sourceTime.getTime());
                            resultTime.setNanos(namiaoadded.intValue());
                        } else {

                        }
                    } else {
                        if (addend < -999999999) {//这里是计算相减，判断相减得值是否超过1000000000即一秒，超过则需要进行调整
                            long na = sourceTime.getNanos();
                            sourceTime = new Timestamp(sourceTime.getTime() + addend / 1000000000 * 1000);
                            sourceTime.setNanos((int) na);
                            addend = addend % 1000000000;
                        }
                        if( sourceTime.getNanos() <Math.abs(addend)){//判断是否小于相减的数，如果小于，则需要借位
                            long na = sourceTime.getNanos();
                            resultTime = new Timestamp(sourceTime.getTime() - (long) 1 * 1000);//借位一秒
                            na = na + 1000000000;
                            na = na + addend;//调整纳秒后，进行计算
                            resultTime.setNanos((int) na);
                        }
                        else {//没有借位，直接计算。
                            long na = sourceTime.getNanos();
                            resultTime = new Timestamp(sourceTime.getTime());
                            na = na + addend;
                            resultTime.setNanos((int) na);
                        }
                    }
                    break;
                case YEAR:
                   if(yyyyMMddhhmmsshhnn[0]+ addend.intValue()<0) {
                       in.put(ERROR_KEY, new DataCell(TDataType.STRING,"减年数"+ (-addend.intValue())+"不能大于初始年数"+yyyyMMddhhmmsshhnn[0]));
                       return;
                   }
                    cc.add(Calendar.YEAR, addend.intValue());
                    resultTime = new Timestamp(cc.getTime().getTime());
                   resultTime.setNanos(sourceTime.getNanos());

                    break;
                case MONTH:
                    cc.add(Calendar.MONTH, addend.intValue());
                    resultTime = new Timestamp(cc.getTime().getTime());
                    resultTime.setNanos(sourceTime.getNanos());
                    break;
                case DAY:
                    cc.add(Calendar.DAY_OF_MONTH, addend.intValue());
                    resultTime = new Timestamp(cc.getTime().getTime());
                    resultTime.setNanos(sourceTime.getNanos());
                    break;
                case HOURS:
                    resultTime = new Timestamp(sourceTime.getTime()+addend.longValue()*3600000);
                    resultTime.setNanos(sourceTime.getNanos());
                    break;
                case MINUTES:
                    resultTime = new Timestamp(sourceTime.getTime()+addend.longValue()*60000);
                    resultTime.setNanos(sourceTime.getNanos());
                    break;
                case SECONDS:
                    resultTime = new Timestamp(sourceTime.getTime()+addend.longValue()*1000);
                    resultTime.setNanos(sourceTime.getNanos());
                    break;
                case MS:
                    resultTime = new Timestamp(sourceTime.getTime()+addend.longValue());
                    resultTime.setNanos(resultTime.getNanos()+sourceTime.getNanos()%1000000);
                default:
            }
            result.setData(resultTime);
            in.put(outKeyList.get(0),result);
        }
    },
    MAPPUT {
        public Map cloneMap(Map map){
           HashMap<Object,Object>res = new HashMap();
           for(Object k : map.keySet()){
               res.put(k,map.get(k));
            }
          return  res;
        }

        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.MAP, null);
            if (inKeyList.size() != 3) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"MAPPUT参数不是3个"));
                return;
            }
            DataCell dc0 = getDataCell(1,inKeyList.get(1),in,tTransformation);//原始的map
            DataCell dc1 = getDataCell(2,inKeyList.get(2),in,tTransformation);//put的键
            DataCell dc2 = getDataCell(3,inKeyList.get(3),in,tTransformation);//put的值
            Map map = cloneMap((Map)dc0.getData());
            if(dc1==null || dc1.getData() ==null) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"MAPPUT的键不能是null"));
                return;
            }
            Object key = dc1.getData();
            Object value = null;
            if(dc2!=null) {
                value = dc2.getData();
            }
            map.put(key,value);
            result.setData(map);
            in.put(outKeyList.get(0), result);
        }
    },
    MAPGET { //由键获取值，返回值是由UI选择
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            Map<String, Object> infoMap = tTransformation.getInfoMap();
            Integer systemData = (Integer)infoMap.get(TTransformationInfoType.OUTPUT_DATA_TYPE.dbValue);
            DataCell result = new DataCell(TDataType.sysType2TDataType(systemData), null);
            if (inKeyList.size() != 2) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"MAPGET参数不是2个"));
                return;
            }
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            DataCell dc1 = getDataCell(1,inKeyList.get(1),in,tTransformation);
            Map<String, Object> mapSource = (Map) dc0.getData();
            String key = dc1.getData().toString();
           if (mapSource.size()==0) {
             if (! StringUtils.isBlank(key)) {
                 in.put(ERROR_KEY, new DataCell(TDataType.STRING,"MAPGET下标越界，元素个数是：" + mapSource.size() + ", 下标是：" + key));
                 return;
              }
           }
           if( ! mapSource.keySet().contains(key)){
               in.put(ERROR_KEY, new DataCell(TDataType.STRING,"MAPGET不包含键"+key));
               return;
           }
            Object obj = mapSource.get(key);//返回值
            Object restype =result.getdType();//返回值类型
            if (obj instanceof ArrayList) {
                List list = new ArrayList();
                list.addAll((List) obj);
                obj = list;
            } else if (restype == TDataType.MAP) {
                Map map = new HashMap();
                map.put(key, obj);
                obj = map;
            }
            result.setData(obj);
            in.put(outKeyList.get(0), result);
        }
    },
    MAPREMOVE {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.MAP, null);
            if (inKeyList.size() != 2) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"MAPREMOVE参数不是2个"));
                return;
            }
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);//原始的map
            DataCell dc1 = getDataCell(1,inKeyList.get(1),in,tTransformation);//要move的key
            if (dc0 == null || dc0.getData() == null) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"MAPREMOVE的map不能是null"));
                return;
            }
            if (dc1 == null || dc1.getData() == null) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"MAPREMOVE的key不能是null"));
                return;
            }
            Map mapSource = (Map) dc0.getData();
            Map map = new LinkedHashMap();
            map.putAll(mapSource);
            String key = dc1.getData().toString();
            map.remove(key);
            result.setData(map);
            in.put(outKeyList.get(0), result);
        }
    },
    ARRAYPUT {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.ARRAY, null);
            if (inKeyList.size() != 3) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"ARRAYPUT参数不是3个"));
                return;
            }
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);//原始集合
            DataCell dc1 = getDataCell(1,inKeyList.get(1),in,tTransformation);//要添加元素的键
            DataCell dc2 = getDataCell(2,inKeyList.get(2),in,tTransformation);//要添加元素的值,可以是单个元素，也可以是数组，Map等多个元素
            //List listSource = (List)dc0.getData();
            List listSource = new ArrayList();
            listSource.addAll((List)dc0.getData());
            for(Object obj : listSource){
                if(obj instanceof String){
                    String str = obj==null ? null : obj.toString();
                    if(str!=null){
                        if(!str.startsWith("\"") && !str.endsWith("\"")){
                            str = JsonUtil.toJSONString(str);
                        }
                    }
                    listSource.set(listSource.indexOf(obj),str);
                }
            }
            Object value = null;
            if (dc2 != null) {
                if(dc2.getdType()==TDataType.STRING){
                    String str = dc2.getData()==null ? null : dc2.getData().toString();
                    if(str!=null){
                        if(!str.startsWith("\"") && !str.endsWith("\"")){
                            dc2.setData(JsonUtil.toJSONString(str));
                        } else {
                            dc2.setData(str);
                        }
                    } else {
                        dc2.setData(str);
                    }

                }
                value = dc2.getData();
            }
            if(dc1.getData() == null || dc1.getData().toString() == null){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"ARRAYPUT的元素键不能是null"));
                return;
            }
            if( !isInteger(dc1.getData().toString())) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"ARRAYPUT的元素键值只允许是整数型"));
                return;
            } else if(Integer.parseInt(dc1.getData().toString())<0) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"ARRAYPUT的元素键值只允许是0或者正整数型"));
                return;
            }
            if(dc1!=null&&dc1.getData()!=null&&Integer.parseInt(dc1.getData().toString())!=-1) {
                Integer   key = Integer.parseInt(dc1.getData().toString());
                if(key<0) {
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING,"ARRAYPUT下标不能是负数"+key));
                    return;
                }
                if(key==listSource.size()){ //put的位置等于数组长度，则直接在后面put
                    listSource.add(key, value);
                }else if(key>listSource.size()) {//put的位置大于数组长度，进入异常
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING,"ARRAYPUT下标越界：数组长度是 " +listSource.size()+"， 下标是 " + key));
                    return;
                }
                else {  //如果在数组长度内put，则替换掉原来的元素
                    listSource.set(key, value);
                }
            }else{
                listSource.add(value);
            }
            result.setData(listSource);
            in.put(outKeyList.get(0), result);
        }
    },
    ARRAYGET { //从Array中取出一个或多个元素
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            if (inKeyList.size() != 2) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"ARRAYGET参数不是2个"));
                return;
            }
           // Integer systemData =  in.get(inKeyList.get(0)).getdType().getDataType();//infoMap.get(TTransformationInfoType.OUTPUT_DATA_TYPE.dbValue);
            Integer systemOutputDatatype = (Integer)tTransformation.getInfoMap().get(TTransformationInfoType.OUTPUT_DATA_TYPE.dbValue);
            DataCell result = new DataCell(TDataType.sysType2TDataType(systemOutputDatatype), null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);//ARRAYGET的原始数组
            DataCell dc1 = getDataCell(1,inKeyList.get(1),in,tTransformation);//ARRAYGET的下标
            if( dc0.getData() == null){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"ARRAYGET入参不能是null"));
                return;
            }
            List listSource = new ArrayList();
            for(Object obj : (List)dc0.getData()){
                /*if(obj instanceof String){
                    String str =(obj==null ? null : obj.toString());
                    if(!str.startsWith("\"") && !str.endsWith("\"")){
                        str = JsonUtil.toJSONString(str);
                    }
                    listSource.add(str);
                } else {*/
                    listSource.add(obj);
                //}
            }
            //listSource.addAll((List)dc0.getData());
            if (!isInteger(dc1.getData().toString())) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"ARRAYGET的元素键值只允许是整数型"));
                return;
            } else if (Integer.parseInt(dc1.getData().toString()) < 0) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"ARRAYGET的元素键值只允许是0或者正整数型"));
                return;
            }
            int arrsize = listSource.size();
            Integer getidx = Integer.parseInt(dc1.getData().toString());
            if (getidx >= arrsize) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"ARRAYGET数组下标越界：数组长度是 " + arrsize + ",获取的下标是 " + getidx));
                return;
            }
            Object res = listSource.get(getidx);
            if (result.getdType()== TDataType.ARRAY) {
                result.setData(Arrays.asList(res));
            } else if (res instanceof ArrayList) {
                List list = new ArrayList();
                list.addAll((List) res);
                res = list;
                result.setData(res);
            } else if (result.getdType()== TDataType.MAP) {
                Map map = new HashMap();
                map.putAll((Map) res);
                res = map;
                result.setData(res);
//            } else if(result.getdType() == TDataType.TIME) {
//                try {
//                    java.sql.Time time = java.sql.Time.valueOf(res.toString());
//                    result.setData(time);
//                }catch (Exception e) {
//                    in.put(ERROR_KEY, new DataCell(TDataType.STRING, res+"不能转换为TIME类型"));
//                    return;
//                }
            } else if(result.getdType() == TDataType.DATE) {
                try {
                    java.sql.Date date = java.sql.Date.valueOf(res.toString());
                    result.setData(date);
                }catch (Exception e) {
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING, res+"不能转换为DATE类型"));
                    return;
                }
            } else if(result.getdType() == TDataType.TIMESTAMP) {
                try {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    java.sql.Timestamp ts = DateUtil.util2Timestamp(sdf.parse(res.toString()));
                    result.setData(ts);
                }catch (Exception e) {
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING, res+"不能转换为TIMESTAMP类型"));
                    return;
                }
            } else if(result.getdType() == TDataType.BIG_DECIMAL) {
                try {
                    BigDecimal bigDecimal = new BigDecimal(res.toString());
                    result.setData(bigDecimal);
                }catch (Exception e) {
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING, res+"不能转换为DECIMAL类型"));
                    return;
                }
            } else {
                result.setData(res);
            }
            in.put(outKeyList.get(0), result);
        }
    },
    ARRAYREMOVE {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.ARRAY, null);
            if (inKeyList.size() != 2) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"ARRAYREMOVE的参数不是2"));
                return;
            }
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            DataCell dc1 = getDataCell(1,inKeyList.get(1),in,tTransformation);
            if(dc1.getData() == null || dc1.getData().toString() == null){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"ARRAYREMOVE的元素键不能是null"));
                return;
            }
            if( !isInteger(dc1.getData().toString())) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"要移除的元素键值只允许是整数型"));
                return;
            } else if(Integer.parseInt(dc1.getData().toString())<0)
            {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"要移除的元素键值只允许是0或者正整数型"));
                return;
            }
            List listSource =  new ArrayList();
            listSource.addAll((List)dc0.getData());
            if (dc1 != null && dc1.getData() != null) {
                if(Integer.parseInt(dc1.getData().toString())>= listSource.size()){
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING,"ARRAYREMOVE越界，下标是"+dc1.getData() +"，元素个数"+listSource.size()));
                    return;
                }else{
                    listSource.remove(Integer.parseInt(dc1.getData().toString()));
                }
            } else {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING," transform data is illegal!"));
                return;
            }
            result.setData(listSource);
            in.put(outKeyList.get(0), result);
        }
    },
    DATEFORMAT {
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            DataCell result = new DataCell(TDataType.STRING, null);
            Object dateFormat =tTransformation.getInfoMap().get(TTransformationInfoType.DATE_FORMAT.dbValue);
            if (dateFormat != null &&  StringUtils.isBlank(dateFormat.toString())) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "DATEFORMAT异常，日期格式不能是空"));
                 return;
            }
            if(! DSUtil.isNull(dc0) && dateFormat != null){
                result.setData(DateUtil.format(dateFormat.toString(), dc0.getData()));
            }
            in.put(outKeyList.get(0), result);
        }
    },
    SPLIT2ARRAY{
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            //ArrayList<TransformationInputs> inputs = (ArrayList<TransformationInputs>) tTransformation.getInfoMap().get("inputs");
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            // 分割字符不能为空
            if (DSUtil.isNull(dc0)) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"被拆分的字符串不能是null"));
                return;
            }
            DataCell dc1 = getDataCell(1,inKeyList.get(1),in,tTransformation);

            Object delimiter = dc1.getData();
            int splitType = (int) tTransformation.getInfoMap().get(TTransformationInfoType.SPLIT_TYPE.dbValue);
            if (delimiter == null) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"用来分割的条件不能是null"));
                return;
            }
            String regex = (String) delimiter;
            if (splitType == 0) {//按分隔符分割
                //规范化分隔符
                String[] value = null;
                try{
                    value = dc0.getData().toString().split(regex,-1);
                } catch (Exception e){
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING,"分隔符不合法"));
                    return;
                }
                /*if(value!=null){
                    for(int i=0;i<value.length;i++){
                        value[i] = JsonUtil.toJSONString(value[i]);
                    }
                }*/
                in.put(outKeyList.get(0), new DataCell(TDataType.ARRAY, asList(value)));
            } else if (splitType == 1) { //按正则表达式分割
                String dc0str = dc0.getData().toString();
                List list = null;
                try {
                    list = getMatchByRegex(dc0str, regex); // 按正则表达式提取
                } catch (Exception e){
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING,"分隔符不合法"));
                    return;
                }
                //List list = getSplitByRegex(dc0str,regex); // 按正则表达式分割
                in.put(outKeyList.get(0), new DataCell(TDataType.ARRAY, list));
            } else {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"SPLIT2ARRAY 不存在该分割类型：" + splitType));
                return;
            }
        }
    },
    TERMEXTRACT2ARRAY { //按正则表达式提取字符串中所有匹配的字符串，如果没有匹配则返回null
        @Override
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if (DSUtil.isNull(dc0)) {  // 字符不能为空
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"被匹配的字符串不能是null"));
                return;
            }
            if (inKeyList.size() < 2) {
                throw new IllegalArgumentException("没有输入正则表达式");
            }
            Object regExpression = getDataCell(1,inKeyList.get(1),in,tTransformation).getData();//tTransformation.getInfoMap().get(TTransformationInfoType.REG_EXPRESSION.dbValue);
            if (regExpression == null) { //正则表达式是null
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"正则表达式不能是null"));
                return;
            }
            DataCell result = new DataCell(TDataType.ARRAY, null);
            Pattern p = null;
            try{
                p = Pattern.compile((String) regExpression);
            } catch (Exception e){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"正则表达式不合法"));
                return;
            }
            String str = dc0.getData().toString();
            Matcher matcher = p.matcher(str);
            List<Object> list = new ArrayList();
            boolean isMatchered=false;
            while (matcher.find()) {
                isMatchered=true;
                list.add(matcher.group(0));
            }
            if(isMatchered) {
                result.setData(list);
                in.put(outKeyList.get(0), result);
            }
        }
    },
    CSVASSEMBLE{ //组装成CSV
        @Override
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            StringBuilder sb = new StringBuilder();
            int idx=0;
            for(int i=0;i<inKeyList.size();i++){
                int ik = inKeyList.get(i);
                if (idx == 0) {
                    idx += 1;
                } else {
                    sb.append(","); // 是null,也要加逗号
                }
                if (getDataCell(i,ik,in,tTransformation) != null && getDataCell(i,ik,in,tTransformation).getData() != null) {
                    sb.append(getDataCell(i,ik,in,tTransformation).getData().toString());
                }
            }
            in.put(tTransformation.getOutKeyList().get(0), new DataCell(TDataType.CSV, sb.toString()));
        }
    },
    RULESQUERY{  //查询关联规则
        @Override
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            Map<String, Object> infoMap = tTransformation.getInfoMap();
          //  SquidTypeEnum squidTypeEnum = (SquidTypeEnum) infoMap.get(TTransformationInfoType.PREDICT_MODEL_TYPE.dbValue);
          //  Object  modelObj = infoMap.get(TTransformationInfoType.PREDICT_DM_MODEL.dbValue);
            DataCell result = new DataCell(TDataType.ARRAY, null);
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation);//前项
            DataCell dc1 = getDataCell(1,inKeyList.get(1),in,tTransformation);//前项中元素数量
            DataCell dc2 = getDataCell(2,inKeyList.get(2),in,tTransformation);//后项
            DataCell dc3 = getDataCell(3,inKeyList.get(3),in,tTransformation);//后项中元素数量
            DataCell dc4 = getDataCell(4,inKeyList.get(4),in,tTransformation);//最小可信度
            DataCell dc5 = getDataCell(5,inKeyList.get(5),in,tTransformation);//最小规则支持度
            DataCell dc6 = getDataCell(6,inKeyList.get(6),in,tTransformation);//最小提升度
            DataCell dc7 = getDataCell(7,inKeyList.get(7),in,tTransformation);//规则数量
            String associationRulesModelTableName = infoMap.get("AssociationRulesModelTableName").toString();//关联规则模型数据表名
            String associationRulesModelIsKey = infoMap.get("AssociationRulesModelIsKey").toString();
         //   HashMap<Integer,String> rulesqueryReferenceColumnIdAndName = (HashMap<Integer,String>)infoMap.get("RulesqueryReferenceColumnIdAndName");
            Integer associationRulesModelVersion = Integer.valueOf(infoMap.get("AssociationRulesModelVersion").toString());
         //   List<Map<String, Object>> associationRulesModel =  (List<Map<String, Object>> )(infoMap.get(TTransformationInfoType.PREDICT_DM_MODEL.dbValue));// 模型
            String antecedent = (dc0 == null ? null : dc0.getData().toString());
            Integer antecedentSize = (dc1 == null ? 0 : Integer.valueOf(dc1.getData().toString()));
            String consequent = (dc2 == null ? null : dc2.getData().toString());
            Integer consequentSize = (dc3 == null ? 0 : Integer.valueOf(dc3.getData().toString()));
            Double minConfidence = (dc4 == null ? 0.0 : Double.valueOf(dc4.getData().toString()));
            Double minRuleSupport = (dc5 == null ? 0.0 : Double.valueOf(dc5.getData().toString()));
            Double minLift = (dc6 == null ? 0.0 : Double.valueOf(dc6.getData().toString()));
            Integer ruleSize = (dc7 == null ? -1 : Integer.valueOf(dc7.getData().toString()));

            //Id name 转为 name value
         /*   HashMap<String,Object> rulesqueryNameValue = new HashMap<>();
            for(Integer refcolumnid : rulesqueryReferenceColumnIdAndName.keySet()) {
                rulesqueryNameValue.put(rulesqueryReferenceColumnIdAndName.get(refcolumnid), in.get(refcolumnid).getData());
            } */
            try {
                RulesQuery rulesQuery = new RulesQuery();
                rulesQuery.antecedent_$eq(antecedent);
                rulesQuery.antecedentSize_$eq(antecedentSize);
                rulesQuery.consequent_$eq(consequent);
                rulesQuery.consequentSize_$eq(consequentSize);
                rulesQuery.minConfidence_$eq(minConfidence);
                rulesQuery.minRuleSupport_$eq(minRuleSupport);
                rulesQuery.minLift_$eq(minLift);
                rulesQuery.ruleSize_$eq(ruleSize);
                rulesQuery.modelVersion_$eq(associationRulesModelVersion);
                rulesQuery.key_$eq(associationRulesModelIsKey);
                String[] queryResult = rulesQuery.rulesQueryCSVFromDB(associationRulesModelTableName);
                java.lang.StringBuilder sb = new java.lang.StringBuilder();
                if(queryResult.length>0) {
                    if (queryResult.length > 1) {
                        for (int i = 0; i < queryResult.length - 1; i++) {
                            sb.append(queryResult[i] + "\n");
                        }
                    }
                    sb.append(queryResult[queryResult.length - 1]); // 最后一个不加换行
                }
                result.setData(Arrays.asList(sb));
                in.put(outKeyList.get(0), result);
            }catch (Throwable e){
                result.setdType(TDataType.STRING);
                result.setData("查询关联规则错误"+e.getMessage());
                in.put(ERROR_KEY, result);
            }
        }
    } ,
    BINARYTOSTRING{
        // 二进制转换为字符串
        // 入参：1）binary或varbinary；2）字符编码
        // 出参：转换后的字符串
        @Override
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            DataCell result = new DataCell(TDataType.STRING, null);
            if (inKeyList.size() != 1) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "BINARYTOSTRING的参数不是1个"));
                return;
            }
            DataCell dc0 = getDataCell(0,inKeyList.get(0),in,tTransformation); // 二进制
            if (dc0.getdType() != TDataType.VARBINARY) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "BINARYTOSTRING的第一个入参类型不VARBINARY"));
                return;
            }
            if (dc0 == null || DSUtil.isNull(dc0)) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "BINARYTOSTRING的第一个入参不能是null"));
                return;
            }
            String encoding = null;
            try {
                int encodingIdx = (int) tTransformation.getInfoMap().get(TTransformationInfoType.ENCODING.dbValue);
                encoding = getEncoding(encodingIdx);
                byte[] bytes = (byte[]) dc0.getData();
                result.setData(new String(bytes, encoding.trim()));
                in.put(outKeyList.get(0), result);
            } catch (ClassCastException e) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, dc0 + "是" + dc0.getdType() + "类型,不能转换为byte类型"));
            } catch (UnsupportedEncodingException e) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "不支持该编码:" + encoding));
            } catch (Exception e) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, e.getMessage()));
            }
        }
    }, CST {
        @Override
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            Integer systemoutputDatatype = (Integer) tTransformation.getInfoMap().get(TTransformationInfoType.OUTPUT_DATA_TYPE.dbValue);
            DataCell input = getDataCell(0,inKeyList.get(0),in,tTransformation);
            if(DSUtil.isNotNull(input) && systemoutputDatatype != null) {
                TDataType outDataType = TDataType.sysType2TDataType(systemoutputDatatype);
                try {
                    Object outObj = TColumn.toTColumnValue(input.getData().toString(), outDataType);
                    in.put(outKeyList.get(0), new DataCell(outDataType, outObj));
                } catch (Exception e) {
                    in.put(ERROR_KEY, new DataCell(TDataType.STRING, e.getMessage()));
                }
            }
        }
    },INVERSENORMALIZER{
        @Override
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            Integer outKey = tTransformation.getOutKeyList().get(0);
            DataCell inDataCell = getDataCell(0,tTransformation.getInKeyList().get(0),in,tTransformation);
            String inPara = inDataCell.getData().toString();
            if(inPara == null || inPara.trim().equals("")){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"逆标准化数据是null或空字符串"));
                return;
            }
            NormalizerModel normalizerModel = (NormalizerModel)tTransformation.getInfoMap().get(TTransformationInfoType.PREDICT_DM_MODEL.dbValue);
            if(normalizerModel == null) {
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,"逆标准化模型是null"));
                return;
            }
            try {
                //String inverse = normalizerModel.inverseNormalizer(inPara);
             //   in.put(outKey, new DataCell(TDataType.CSN, inverse));
                DataCell result = DSUtil.inverseNormalizer(normalizerModel, inDataCell);
                in.put(outKey, result);
            }catch (Throwable e){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING,e.getMessage()));
                return;
            }
        }
    },NVL{
        // 如果v1为null， 则输出v2, 否则输出v1
        @Override
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            DataCell dc0 = getDataCell(0,tTransformation.getInKeyList().get(0),in,tTransformation);
            DataCell dc1 = getDataCell(1,tTransformation.getInKeyList().get(1),in,tTransformation);
            if(dc0 == null || dc0.getData() == null){
                if(dc1 == null || dc1.getData() == null){
                    try {
                        TDataType type = TDataType.sysType2TDataType(Integer.valueOf(tTransformation.getInfoMap().get(TTransformationInfoType.OUTPUT_DATA_TYPE.dbValue) + ""));
                        in.put(tTransformation.getOutKeyList().get(0), new DataCell(type, null));
                    } catch (Exception e){
                        in.put(ERROR_KEY, new DataCell(TDataType.STRING, e.getMessage()));
                    }
                } else {
                    in.put(tTransformation.getOutKeyList().get(0),new DataCell(dc1.getdType(),dc1.getData()));
                }
            } else {
                in.put(tTransformation.getOutKeyList().get(0),new DataCell(dc0.getdType(),dc0.getData()));
            }
        }
    },DATETOSTRING {
        @Override
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            DataCell dc0 = getDataCell(0,tTransformation.getInKeyList().get(0),in,tTransformation);
            if(dc0!=null && dc0.getData()!=null){
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                String dateStr = dateFormat.format(dc0.getData());
                in.put(tTransformation.getOutKeyList().get(0),new DataCell(TDataType.STRING,dateStr));
            } else {
                in.put(tTransformation.getOutKeyList().get(0),new DataCell(TDataType.STRING,null));
            }
        }
    },NULLPERCENTAGE {
        @Override
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> inKeyList = tTransformation.getInKeyList();
            if(inKeyList==null || inKeyList.size()==0){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "NullPercentage入参为空"));
            } else {
                int nullNum=0;
                for(int i=0;i<inKeyList.size();i++){
                    int inKey = inKeyList.get(i);
                    DataCell dc  = getDataCell(i,inKey,in,tTransformation);
                    if(dc==null || dc.getData()==null){
                        nullNum+=1;
                    }
                }
                //计算比例,并格式化为四位有效数字
                DecimalFormat decimalFormat = new DecimalFormat("#.####");
                String percent = decimalFormat.format(MathUtil.div(nullNum*1.0,inKeyList.size())*100);
                TDataType outType = TDataType.sysType2TDataType(Integer.valueOf(tTransformation.getInfoMap().get(TTransformationInfoType.OUTPUT_DATA_TYPE.dbValue)+""));
                Object outObj = TColumn.toTColumnValue(percent, outType);
                in.put(tTransformation.getOutKeyList().get(0),new DataCell(outType,outObj));
            }
        }
    },UUID {
        @Override
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            List<Integer> outKeyList = tTransformation.getOutKeyList();
            String uuid = java.util.UUID.randomUUID().toString();
            DataCell result = new DataCell(TDataType.STRING, uuid);
            in.put(outKeyList.get(0), result);
        }
    },ENCRYPT {
        @Override
        public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
            Map<String,Object> infoMap = tTransformation.getInfoMap();
            Object v = infoMap.get(TTransformationInfoType.ENCRYPT.dbValue);
            //加密类型不能为空
            if(v==null){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "加密类型不能为空"));
                return;
            }
            // 入参不能为空
            List<Integer> inKeyList = tTransformation.getInKeyList();
            if(inKeyList==null || inKeyList.size()==0){
                in.put(ERROR_KEY, new DataCell(TDataType.STRING, "ENCRYPT入参为空"));
                return;
            }
            // 入参的值为空，不加密
            DataCell dc = getDataCell(0,tTransformation.getInKeyList().get(0),in,tTransformation);;
            //DataCell dc = in.get(tTransformation.getInKeyList().get(0));
            if(dc == null){
                in.put(tTransformation.getOutKeyList().get(0),dc);
            } else {
                String value = dc.getData().toString();
                String mdValue = null;
                int type = Integer.parseInt(v.toString());
                try {
                    switch (type) {
                        case 0:
                            mdValue = DigestUtils.md2Hex(value);
                            break;
                        case 1:
                            mdValue = DigestUtils.md5Hex(value);
                            break;
                        case 2:
                            mdValue = DigestUtils.sha1Hex(value);
                            break;
                        case 3:
                            mdValue = DigestUtils.sha256Hex(value);
                            break;
                        case 4:
                            mdValue = DigestUtils.sha384Hex(value);
                            break;
                        case 5:
                            mdValue = DigestUtils.sha512Hex(value);
                            break;
                    }
                } catch (Exception e){
                    e.printStackTrace();
                }
                in.put(tTransformation.getOutKeyList().get(0),new DataCell(TDataType.STRING,mdValue));
            }

        }
    };

    public DataCell getDataCell(int index,Integer indexValue,Map<Integer, DataCell> in,TTransformation tf){
        ArrayList<TransformationInputs> inputList = (ArrayList<TransformationInputs>) tf.getInfoMap().get("inputs");
        DataCell dataCell = null;
        if(indexValue==0){
            if(inputList != null && inputList.size() > 0){
                TransformationInputs input = inputList.get(index);
                String value = input.getInput_value();
                String[] dataTypeArr = input.getInput_Data_Type().split(",");
                TDataType dataType = TDataType.sysType2TDataType(Integer.parseInt(dataTypeArr[0]));
                if(input.getSource_type() == 1){
                    Object dataCellValue = TColumn.toTColumnValue(value,dataType);
                    dataCell = new DataCell(dataType,dataCellValue);
                } else if(input.getSource_type() == 2){
                    dataCell = null;
                }
            }
        } else {
            dataCell = in.get(indexValue);
        }

        return dataCell;
    }

    public static DataCell getDataCell2(int index,Integer indexValue,Map<Integer, DataCell> in,Map<String, Object> infoMap){
        ArrayList<TransformationInputs> inputList = (ArrayList<TransformationInputs>) infoMap.get("inputs");
        DataCell dataCell = null;
        if(indexValue==0){
            if(inputList != null && inputList.size() > 0){
                TransformationInputs input = inputList.get(index);
                String value = input.getInput_value();
                String[] dataTypeArr = input.getInput_Data_Type().split(",");
                TDataType dataType = TDataType.sysType2TDataType(Integer.parseInt(dataTypeArr[0]));
                if(input.getSource_type() == 1){
                    Object dataCellValue = TColumn.toTColumnValue(value,dataType);
                    dataCell = new DataCell(dataType,dataCellValue);
                } else if(input.getSource_type() == 2){
                    dataCell = new DataCell(dataType,null);
                }
            }
        } else {
            dataCell = in.get(indexValue);
        }

        return dataCell;
    }

    private static Object getModleObject(Map<Integer, DataCell> in, List<Integer> inKeys, Map<String, Object> infoMap,
                                         SquidTypeEnum squidTypeEnum,String modelSql ) {
        Object modelObj = null;
        if(inKeys.size()==1) {
         //   modelObj = infoMap.get(TTransformationInfoType.PREDICT_DM_MODEL.dbValue);
            modelObj = infoMap.get(modelSql);
        } else if(inKeys.size()==2) {
            // 存在key
            String sql = (String)infoMap.get(modelSql);
            DataCell keyDC = getDataCell2(1,inKeys.get(1),in,infoMap);
            Object model_ = TransformationModelThreadLocal.existModel(keyDC.getData());
            if(model_ == null) {
                List<byte[]> mbytes = null;
                if(DSUtil.isNull(keyDC)) {
                    mbytes = ConstantUtil.getDataMiningJdbcTemplate().query(sql.replace("= ?"," is null "), new RowMapper<byte[]>() {
                        @Override
                        public byte[] mapRow(ResultSet rs, int rowNum) throws SQLException {
                            return rs.getBytes(1);
                        }
                    });
                } else {
                    mbytes = ConstantUtil.getDataMiningJdbcTemplate().query(sql,
                            new RowMapper<byte[]>() {
                                @Override
                                public byte[] mapRow(ResultSet rs, int rowNum)
                                        throws SQLException {
                                    return rs.getBytes(1);
                                }
                            }, new Object[] { keyDC.getData() });
                }
                if(mbytes.size()==0) {
                    throw new EngineException("该数据挖掘模型不存在,key为:" + DataCellUtil.getData(keyDC));
                }
                modelObj = SquidUtil.genModel(mbytes.get(0), squidTypeEnum);
                TransformationModelThreadLocal.setModel(keyDC.getData(), modelObj);
            }  else {
                modelObj = model_;
            }
        }
        return modelObj;
    }

    /**
     *
     * @param encodingIndex
     * @return
     */
    private static String getEncoding(int encodingIndex) {
        String[] encodings = new String[]{
                "UTF-8",
                "ASCII",
                "GBK",
                "UNICODE",
                "GB2312 ",
                "BIG5"};
        if (encodingIndex >= encodings.length){
             throw new RuntimeException("字符编码下标超过"+encodings.length);
        }
       return encodings[encodingIndex];
    }

    /**
     * constant输入时前后有单引号，
     * @param items
     * @return
     */
    private static String removeStartAndEndSingleQuotes(String items){
        if(items.trim().startsWith("'") && items.trim().endsWith("'")){
            return items.substring(1,items.length()-1);
        }else {
            return items;
        }
    }

    /**
     * 去除字符串左边的空白符
     * @param str
     * @return
     */
    private static String trimStart(String str){
        if(str.length()==0){
            return str;
        }if(StringUtils.isWhitespace(str)) { //空白字符且长度不等于0
            return str.trim();
        }
        int i = 0;
        while (i < str.length() && Character.isWhitespace(str.charAt(i))) {
            i++;
        }
        return str.substring(i);
    }

    /**
     * 判断一个字符串是不是一个合法的日期格式
     * @param str  格式 2016-04-01 16:56:18 或  2016-04-01 16:56:18.666666666
     * @return
     */
    private static boolean isyyyyMMDDHHmmssFormat(String str) {
        boolean convertSuccess = true;
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            format.setLenient(false);
            format.parse(str);
        } catch (ParseException e) {
            convertSuccess = false;
        }
        return convertSuccess;
    }
    /**
     * @param jsonText
     * @return
     */
    public static Map parseMap(String jsonText) {
        return  (Map)JSON.parse(jsonText);
    }

    /**
     * @param jsonText
     * @return    返回 List<Object>
     */
    public static List<Object> parseArray(String jsonText) {
      return JSON.parseArray(jsonText);
    }

    /**
     * 是否是直辖市
     * @param city
     * @return
     */
    private static boolean isChinaMunicipality(String city) {
        String[] municipality = {"北京", "北京市", "中国北京", "中国北京市", "京","北京北京市","北京北京",
                "上海", "上海市", "中国上海", "中国上海市", "沪","上海上海市","上海上海",
                "天津", "天津市", "中国天津", "中国天津市", "津","天津天津市","天津天津",
                "重庆", "重庆市", "中国重庆", "中国重庆市", "巴", "渝","重庆重庆市","重庆重庆"};
        String citytmp = city.trim();
        for (String cy : municipality) {
            if (cy.equals(citytmp)) {
                return true;
            }
        }
        return false;
    }

    /**
     * string 格式转换为 Timestamp 格式,，带有 毫秒 纳秒
     * @param dtstr  格式1234-01-02 12:34:56.123456789
     * @return
     */
    public static java.sql.Timestamp StringToTimestampWithNaso(String dtstr) {
        String[] sped = dtstr.split(" ");
        int year=0;
        int month=0;
        int date=0;
        if(dtstr.startsWith("-")){ //以-开头，表示有负的年份，
            String[] dates = sped[0].split("-");//年月日
            year = -NumberUtils.toInt(dates[1].trim());
            month = NumberUtils.toInt(dates[2].trim());
            date = NumberUtils.toInt(dates[3].trim());
        }else {
             String[] dates = sped[0].split("-");//年月日
              year = NumberUtils.toInt(dates[0].trim());
              month = NumberUtils.toInt(dates[1].trim());
             date = NumberUtils.toInt(dates[2].trim());
        }

        String[] times = sped[1].split(":");//冒号分割时分秒纳秒
        if(times.length<3){
            times = sped[1].split(";");//分号分割 时分秒纳秒
        }
        int hour = NumberUtils.toInt(times[0].trim());
        int minit = NumberUtils.toInt(times[1].trim());
        String se = times[2];
        String[] miaonaiao = se.split("\\.");//分割 秒.纳秒
        int second = NumberUtils.toInt(miaonaiao[0].trim()); //秒
        int namiao =0;
        if(miaonaiao.length==2) {
            namiao = NumberUtils.toInt(miaonaiao[1].trim()); //纳秒
            int length = String.valueOf(namiao).length();
            for(int i=1;i<=9-length;i++){ //补全9位数，共9位，前面3位是毫秒，后面6位是纳秒
                namiao=10*namiao;
            }
        }
        return new java.sql.Timestamp(year - 1900, month - 1, date, hour, minit, second, namiao);
    }


    /**
     * 获取时间格式中的  年，月，日，时，分，秒，毫秒，纳秒
     *  @param timestamp  格式 1234-01-02 12:34:56.123456789
     * @return
     */
    public static Long[] parseTimestamp(String timestamp) {
        Long year = 0L;
        Long month = 0L;
        Long day =0L;
        Long hour =0L;
        Long minute= 0L;
        Long second = 0L;
        Long millisecond =0L;
        Long nanosecond = 0L;
        String[] sped = timestamp.split(" ");
        String[] dates = sped[0].split("-");//年月日
        year = NumberUtils.toLong(dates[0].trim());
        month = NumberUtils.toLong(dates[1].trim());
        day = NumberUtils.toLong(dates[2].trim());

        String[] times = sped[1].split(":");//冒号分割时分秒纳秒
        if (times.length < 3) {
            times = sped[1].split(";");//分号分割 时分秒纳秒
        }
        hour = NumberUtils.toLong(times[0].trim());
        minute = NumberUtils.toLong(times[1].trim());
        String se = times[2];
        String[] miaonaiao = se.split("\\.");//分割 秒.纳秒
        second = NumberUtils.toLong(miaonaiao[0].trim()); //秒
        if (miaonaiao.length == 2) {//有毫秒或纳秒
            long haomiaonamiao = NumberUtils.toLong(miaonaiao[1].trim()); //毫秒或纳秒
            String ddstr = String.valueOf(haomiaonamiao);
            int length = ddstr.length();
            if (length <= 3) {//只有毫秒
                millisecond = haomiaonamiao;
            } else { //有纳秒  前面3位是毫秒，后面6位是纳秒
                //取前面3位是毫秒
                nanosecond = haomiaonamiao; //纳秒
                for (int i = 1; i <= 9 - length; i++) { //后面补全9位数，共9位，前面3位是毫秒，后面6位是纳秒
                    nanosecond = 10 *nanosecond  ;
                }
                String namiaostr = String.valueOf(nanosecond);
                millisecond = Long.valueOf(namiaostr.substring(0, 3));
                nanosecond = Long.valueOf(namiaostr.substring(3, 9));
            }
        }
        return new Long[]{year, month, day, hour, minute, second, millisecond, nanosecond};
    }

    /**
     * 去除字符串右边的空白符
     * @param str
     * @return
     */
    private static String trimEnd(String str){
        if (str.length() == 0) {
            return str;
        }
        if (StringUtils.isWhitespace(str)) { //空白字符且长度不等于0
            return str.trim();
        }
        int i = str.length() - 1;
        while (i >= 0 && Character.isWhitespace(str.charAt(i))) {
            i--;
        }
        return str.substring(0, i+1);
    }

    /**
     * a 除以b的商和余数
     * @param a
     * @param b
     * @return
     */
    public static Long[] shangyushu(Long a,Long b) {
        if(a==0L){
            return new Long[]{0L, 0L};
        }
        if(b==0L){
            throw new IllegalArgumentException("b不能是0");
        }
        Long yushu = a % b;
        Long shang = (a - yushu) / b;
        return new Long[]{shang, yushu};
    }

    private static double tolerance = Double.MIN_VALUE;//浮点数计算的容差

    public void transferring(Map<Integer, DataCell> in, TTransformation tTransformation) {
        throw new AbstractMethodError();
    }

    private static Map<TransformationTypeEnum, TransformationTypeAdaptor> map = new HashMap<>();
    private final static String FULL_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    private final static String FULL_TIME_FORMATNoNaos = "yyyy-MM-dd HH:mm:ss";//不含毫秒，纳秒
    private static Log logger = LogFactory.getLog(TransformationTypeAdaptor.class);
    private static Random random = new Random();

    static {
        for (TransformationTypeEnum typeEnum : TransformationTypeEnum.values()) {
            map.put(typeEnum, TransformationTypeAdaptor.valueOf(typeEnum.name()));
        }
    }

    public static void mapping(Map<Integer, DataCell> in, TTransformation tTransformation) {
        boolean success = false;
        // 判断当前transformation是否执行
        TFilterExpression filterExpression = tTransformation.getFilterExpression();
        if (filterExpression != null) {
            if (ExpressionValidator.validate(filterExpression, in)) {
                success = true;
            }
        } else {
            success = true;
        }
        if (success) {
            map.get(tTransformation.getType()).transferring(in, tTransformation);
        } else {
            for (Integer key : tTransformation.getOutKeyList()) {
                in.put(key, null); //不满足条件时，跳过该transformation
            }
        }
    }

    /**
     * 判断字符串是否是整数
     * @param str
     * @return
     */
    public static boolean isInteger(String str ) {
        try {
            int num = Integer.valueOf(str.trim());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 是否是数字
     * @param str
     * @return
     */
    public static boolean isNumber(String str){
        if(str == null || StringUtils.isWhitespace(str)){
           return false;
        }
        return str.trim().matches("^[-+]?(([0-9]+)([.]([0-9]+))?|([.]([0-9]+))?)$") || isScientificNotation(str);
    }

    /**
     * 是否是科学计数法
     * @param str
     * @return
     */
    public static boolean isScientificNotation(String str){
        String regx = "^((-?\\d+.?\\d*)[Ee]{1}(-?\\d+))$";
        Pattern pattern = Pattern.compile(regx);
        return pattern.matcher(str).matches();
    }

    /**
     * 四舍五入 保留小数
     *需要保留小数的double值
     * @param value
     * @param num   需要保留的位数
     * @return double
     */
    public static double DoubleRound(Double value, int num) {
        if (value == null) {
            return 0d;
        }
        BigDecimal bigDecimal = new BigDecimal(value);
        value = bigDecimal.setScale(num, BigDecimal.ROUND_HALF_UP).doubleValue();
        return value;
    }

    /**
     * 纳秒 相加减
     * @param yyyyMMddHHmmsshhnn 年 月 日 时 分 秒 毫秒 纳秒
     * @param incUnit 增减的时间单位，
     * @param invValue  增减值
     * @return 年 月 日 时 分 秒 毫秒 纳秒
     */
    public static Long[] getDateTimeIncrease(Long[] yyyyMMddHHmmsshhnn,int incUnit,long invValue) {
       /* switch (incUnit) {
            case 7: //纳秒
                yyyyMMddHHmmsshhnn[7] += invValue;
                break;
        }*/
        yyyyMMddHHmmsshhnn[7] += invValue;  //纳秒
        yyyyMMddHHmmsshhnn = normalizeyyyyMMddHHmmsshhnn(yyyyMMddHHmmsshhnn);
        return yyyyMMddHHmmsshhnn;
    }

    /**
     * 对超出时间范围的数值进行规范化时间范围内
     * @param
     * @return
     */
    public static Long[] normalizeyyyyMMddHHmmsshhnn(Long[] yyyyMMddHHmmsshhnn) {
        boolean isNegtiveOrZero = true;
        for (int i = 0; i < yyyyMMddHHmmsshhnn.length - 1; i++) { // 防止出最后一个不是0，之前的全都是0
            if (yyyyMMddHHmmsshhnn[i] > 0) {
                isNegtiveOrZero = false;
                break;
            }
        }
        if (isNegtiveOrZero) {
            return yyyyMMddHHmmsshhnn;
        }
        if (yyyyMMddHHmmsshhnn[7] < 0) { // 纳秒
            Long[] shangyu = TransformationTypeAdaptor.shangyushu(- yyyyMMddHHmmsshhnn[7],1000000L);
            if(shangyu[0] == 0L){
                yyyyMMddHHmmsshhnn[6] -=1;
            }else {
                yyyyMMddHHmmsshhnn[6] -= shangyu[0]; // 毫秒
            }
            if(shangyu[1] == 0L) {
                yyyyMMddHHmmsshhnn[7] =0L;
            }else {
                if(shangyu[0] == 0L){
                    yyyyMMddHHmmsshhnn[7] = 1000000 + yyyyMMddHHmmsshhnn[7];
                }else {
                    yyyyMMddHHmmsshhnn[7] = shangyu[0] * 1000000 + yyyyMMddHHmmsshhnn[7];
                }
            }
        } else if (yyyyMMddHHmmsshhnn[7] >= 10000000) {
            Long[] shangyu = TransformationTypeAdaptor.shangyushu(yyyyMMddHHmmsshhnn[7],1000000L);
            yyyyMMddHHmmsshhnn[6] += shangyu[0];//毫秒
            if(shangyu[1] == 0L){
                yyyyMMddHHmmsshhnn[7]=0L;
            }else {
                yyyyMMddHHmmsshhnn[7] =  shangyu[1]; // 纳秒
            }
        }
        // GregorianCalendar 会根据数值自动时间的进制的相加减
        GregorianCalendar calendar = new GregorianCalendar();
        calendar.set(Calendar.YEAR, yyyyMMddHHmmsshhnn[0].intValue()-1900);
        calendar.set(Calendar.MONTH, yyyyMMddHHmmsshhnn[1].intValue()-1);
        calendar.set(Calendar.DATE, yyyyMMddHHmmsshhnn[2].intValue()-1);
        calendar.set(Calendar.HOUR_OF_DAY, yyyyMMddHHmmsshhnn[3].intValue());//24小时制
        calendar.set(Calendar.MINUTE, yyyyMMddHHmmsshhnn[4].intValue());
        calendar.set(Calendar.SECOND, yyyyMMddHHmmsshhnn[5].intValue());
        calendar.set(Calendar.MILLISECOND, yyyyMMddHHmmsshhnn[6].intValue());

        yyyyMMddHHmmsshhnn[0] = new Long(calendar.get(Calendar.YEAR));
        yyyyMMddHHmmsshhnn[1] = new Long(calendar.get(Calendar.MONTH));
        yyyyMMddHHmmsshhnn[2] = new Long(calendar.get(Calendar.DATE))+1;
        yyyyMMddHHmmsshhnn[3] = new Long(calendar.get(Calendar.HOUR_OF_DAY));
        yyyyMMddHHmmsshhnn[4] = new Long(calendar.get(Calendar.MINUTE));
        yyyyMMddHHmmsshhnn[5] = new Long(calendar.get(Calendar.SECOND));
        yyyyMMddHHmmsshhnn[6] = new Long(calendar.get(Calendar.MILLISECOND));
        return yyyyMMddHHmmsshhnn;
    }
    /**
     * 正则表达式分割
     * @param sourceString
     * @param regex
     * @return
     */
    public static List<String> getSplitByRegex(String sourceString, String regex){
        Matcher matcher = Pattern.compile(regex).matcher(sourceString);
        int idx = 0;
        List<Integer> matchedStartIndexes = new ArrayList<>(); // 记录匹配的开始下标
        List<Integer> matchedEndIndexes = new ArrayList<>(); // 记录匹配的结束下标
        while(matcher.find()) {
            matchedStartIndexes.add(matcher.start());
            matchedEndIndexes.add(matcher.end());
            idx ++;
        }
        if(idx == 0){
            return asList(sourceString);
        }else {
            List<Integer> saveStartIndexes = new ArrayList<>(); // 保留的开始下标
            for(int i=0;i<sourceString.length();i++){
                if(!isInterval(i, matchedStartIndexes, matchedEndIndexes)){
                    saveStartIndexes.add(i);
                }
            }
            List res = new ArrayList();
            if(saveStartIndexes.size() ==0){ //被分割的源字符串全都相同时，返回空字符串，数量比源字符串长度多一个
                for(int i=0;i<sourceString.length()+1;i++){
                    res.add("");
                }
                return res;
            }
            for(int i=0;i<sourceString.length();i++){
                if(saveStartIndexes.contains(i)){
                   int endnumber = getComtinueEndNumber(i,saveStartIndexes);
                    res.add(sourceString.substring(i,endnumber+1));
                    i= endnumber;
                }else{
                    res.add("");
                }
            }
            return res;
        }
    }

    /**
     *  返回不连续数字的最后一个数字
     *  如 1 2 3 7 8 9
     *  startNumber = 1，时，返回3
     *  startNumber = 7，时，返回9
     * @param startNumber
     * @param numbers
     * @return
     */
    public static int getComtinueEndNumber(int startNumber, List<Integer> numbers ){
        if( numbers.indexOf(startNumber) == numbers.size()-1){
            return startNumber;
        }
        for(int i= numbers.indexOf(startNumber);i<numbers.size();i++) {
            int k=1;
            for (int j = i + 1; j < numbers.size(); j++) {
                if (numbers.get(i) != numbers.get(j) - k) {
                    return numbers.get(j-1);
                }
                if(j==numbers.size()-1){
                    return numbers.get(j);
                }
                k++;
            }
        }
      return -1;
    }
    private static boolean isInterval(int number, List<Integer> from , List<Integer> to){
        for(int i=0;i<from.size();i++){
            if(number>= from.get(i) && number< to.get(i) ) {
                return true;
            }
        }
        return false;
    }

    /**
     * 按正则表达式提取
     * @param sourceString
     * @param regex
     * @return
     */
    public static List<String> getMatchByRegex(String sourceString, String regex){
        Matcher matcher = Pattern.compile(regex).matcher(sourceString);
        List<String> list= new ArrayList();
        while(matcher.find()) {
            list.add(matcher.group(0));
        }
        return list;
    }

    /**
     * 按照sqlserver的substring
     * @param sourceStr
     * @param startIndex  开始位置，从0开始
     * @param cutoffLength  截取长度，从0开始
     * @return
     */
    public static String substringLikeSQLServer(String sourceStr,int startIndex,int cutoffLength){
        if(sourceStr == null){
            throw new IllegalArgumentException("截取字符串不能是null");
        }
        if(cutoffLength<0){
            throw new IllegalArgumentException("截取长度不能是负数");
        }
        if(cutoffLength==0){
            return "";
        }
        if(startIndex ==0 && cutoffLength==0) {
            return "";
        }
        if(startIndex<0){
            int tmp = startIndex + cutoffLength;
            if (tmp<=1){
               return "";
            }else {
                startIndex = startIndex-1;
                int endidx = startIndex+cutoffLength ;
                if(endidx>sourceStr.length()){
                    endidx = sourceStr.length();
                }
                return sourceStr.substring(0,endidx);
            }
        }else if(startIndex == 0){
             if(cutoffLength<=1){
                 return "";
             }else{
                 int endidx = cutoffLength-1;
                 if (endidx > sourceStr.length()) {
                     endidx = sourceStr.length();
                 }
                 return sourceStr.substring(startIndex, endidx);
             }
        }else {
            if(startIndex>sourceStr.length()){
                return "";
            }
            startIndex = startIndex - 1;
            int endidx = startIndex + cutoffLength;
            if (endidx > sourceStr.length()) {
                endidx = sourceStr.length();
            }
            return sourceStr.substring(startIndex, endidx);
        }
    }

    public static void main(String[] args){
        String msg = "".trim();
        System.out.println(msg);
        System.out.println(JSON.parseArray("[\"f\",\"2\"]"));
    }
}

class TransformationModelThreadLocal {

    private static ThreadLocal<Map<Object, Object>> models;

    private static Map<Object, Object> getModel() {
        return models.get();
    }

    public static void setModel(Object key, Object value) {
        if(models.get() == null)  {
            models.set(new WeakHashMap<Object, Object>());
        }
        models.get().put(key, value);
    }

    public static Object existModel(Object key) {
        return getModel() != null ? getModel().get(key) : null;
    }
}