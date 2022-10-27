package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.entity.clean.Cleaner;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.util.DSUtil;
import com.eurlanda.datashire.engine.util.SquidUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * spark引擎中转换类型的squid
 *
 * @author Gene
 */
public class TTransformationSquid extends TExceptionDataSquid {
    transient private static Log log = LogFactory.getLog(TTransformationSquid.class);
    public final static Integer ERROR_KEY = 0;
    // 待做的transformation 操作
    private List<TTransformationAction> tTransformationActions;
    private TSquid previousSquid;

    public TTransformationSquid() {
        this.setType(TSquidType.TRANSFORMATION_SQUID);
    }

    @Override
    public Object run(JavaSparkContext jsc) throws EngineException {
        // 判断是否可以执行
        if (this.previousSquid.getOutRDD() == null) {
            this.previousSquid.runSquid(jsc);
        }
        // 判断是否已经执行
        if (this.outRDD != null) {
            return outRDD;
        }

        log.info("transformation squid 执行：id:" + this.getSquidId());
        // 获得输入数据
        // 待处理的数据集
        final TTransformationSquid me = this;

        // 根据transformationAction切分, common,TOKENIZATION
        JavaRDD<Map<Integer, DataCell>> preRDD = this.previousSquid.getOutRDD();

        boolean isPreviousExceptionSquid  = false;
        if(me.getPreviousSquid().getType() == TSquidType.EXCEPTION_SQUID){
            isPreviousExceptionSquid  = true;
        } else if(me.getPreviousSquid().getType() == TSquidType.FILTER_SQUID
                && ((TFilterSquid)me.getPreviousSquid()).getPreviousSquid().getType() == TSquidType.EXCEPTION_SQUID) {
            isPreviousExceptionSquid  = true;
        }
        outRDD = SquidUtil.transformationRDD(preRDD, tTransformationActions, isPreviousExceptionSquid, me.getJobContext());

        /**
        List<TTransformationAction> tas = null;
        for(TTransformationAction ta : tTransformationActions) {
            TransformationTypeEnum type = ta.gettTransformation().getType();
            Map<String, Object> infoMap = ta.gettTransformation().getInfoMap();
            SquidTypeEnum squidTypeEnum = (SquidTypeEnum)infoMap.get(TTransformationInfoType.PREDICT_MODEL_TYPE.dbValue);
            // 对预测的transformation,查询模型
            int inkeySize = ta.gettTransformation().getInKeyList().size();
            if(type == TransformationTypeEnum.PREDICT
                    || type == TransformationTypeEnum.INVERSEQUANTIFY) {
                List<Integer> inKeys = ta.gettTransformation().getInKeyList();
                if(inkeySize==1) {
                    // 模型类型
                    String sql = (String)infoMap.get(TTransformationInfoType.PREDICT_DM_MODEL.dbValue);
                    Map<String,Object> data = ConstantUtil.getHbaseJdbc().queryForMap(sql);
                    byte[] bytes = (byte[])data.get("model");
                    // 直接放 模型
                    Object model = genModel(bytes, squidTypeEnum);
                    // 广播一下提高性能
                    model = jsc.broadcast(model);
                    infoMap.put(TTransformationInfoType.PREDICT_DM_MODEL.dbValue, model);
                } else if(inkeySize==2) {
                    // 存在key

                }

            }
//            if(type.isCommon(squidTypeEnum)) {
            if(type.isCommon(squidTypeEnum, inkeySize)) {
                if(tas == null) {
                    tas = new ArrayList<>();
                    tas.add(ta);
                } else {
                    tas.add(ta);
                }
            } else {
                if (tas != null) {
                    preRDD = new CommonProcessor(preRDD, tas, me, isExceptionHandle).process(jsc);
                    tas = null;
                }
                // 处理非common的
                switch (type) {
                    case TOKENIZATION:
                        preRDD = new TokenizationProcessor(preRDD, ta, me).process(jsc);
                        break;
                    case AUTO_INCREMENT:
                        preRDD = new AutoIncrementProcessor(preRDD, ta, me).process(jsc);
                        break;
                    case PREDICT:   // als predict
                        if(squidTypeEnum == SquidTypeEnum.ALS) {
                            preRDD = new ALSPredictProcessor(preRDD, ta, me).process(jsc);
                        }
                        // 存在key
                        else if(inkeySize ==2) {
                            preRDD = new PredictWithKeyProcessor(preRDD, ta).process(jsc);
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("不存在该transformation" + type);
                }
            }
        }

        // 扫尾
        if(tas != null) {
            outRDD = new CommonProcessor(preRDD, tas, me, isExceptionHandle).process(jsc);
        } else {
            outRDD = preRDD;
        }
        */

        boolean isNeedCached = DSUtil.isNeedCache(this.getName());

        //filter error data
        if (isExceptionHandle) {
//            outRDD = outRDD.persist(StorageLevel.MEMORY_AND_DISK());
            expOutRDD = outRDD.filter(new Function<Map<Integer, DataCell>, Boolean>() {
                @Override
                public Boolean call(Map<Integer, DataCell> in) throws Exception {
                    return in != null && in.size() != 0 && in.containsKey(ERROR_KEY);
                }
            }).map(new Function<Map<Integer, DataCell>, Map<Integer, DataCell>>() {
                @Override
                public Map<Integer, DataCell> call(Map<Integer, DataCell> in) throws Exception {
                    Map<Integer, DataCell> result = new HashMap<>();
                    for (Map.Entry<Integer, DataCell> entry : in.entrySet()) {
                        if (entry.getKey() <= 0) {
                            result.put(entry.getKey(), entry.getValue());
                        }

                    }
                    if (log.isDebugEnabled()) log.debug("end exception TTransformationSquid: [result:" + result.toString() + "]");
                    return result;
                }
            });

            if(isNeedCached) {
                expOutRDD = DSUtil.cacheRDD(this.getName(), expOutRDD);

                final JavaRDD<Map<Integer, DataCell>> cachedExpOutRDD = expOutRDD;
                this.getCurrentFlow().addCleaner(new Cleaner() {
                    @Override public void doSuccess() {
                        cachedExpOutRDD.unpersist(false);
                    }
                });
            }
        }

        //filter right data
        outRDD = outRDD.filter(new Function<Map<Integer, DataCell>, Boolean>() {
            @Override
            public Boolean call(Map<Integer, DataCell> in) throws Exception {
                boolean isSuccess = in != null && in.size() > 0 &&
                        (!in.containsKey(ERROR_KEY) || (in.containsKey(ERROR_KEY) && previousSquid.getType() == TSquidType.EXCEPTION_SQUID));
//                if(!isSuccess){
//                    log.debug("end TTransformationSquid,filter data.");
//                }
                return isSuccess;
            }
        }).map(new Function<Map<Integer, DataCell>, Map<Integer, DataCell>>() {
            @Override
            public Map<Integer, DataCell> call(Map<Integer, DataCell> in) throws Exception {
                Map<Integer, DataCell> result = new HashMap<>();
                for (Map.Entry<Integer, DataCell> entry : in.entrySet()) {
                    if (entry.getKey() >= 0) {
                        result.put(entry.getKey(), entry.getValue());
                    }
                }
//                if (log.isDebugEnabled()) log.debug("end TTransformationSquid: [result:" + result.toString() + "]");
                return result;
            }
        });

        if(isNeedCached) {
            outRDD = DSUtil.cacheRDD(this.getName(), outRDD);

            final JavaRDD<Map<Integer, DataCell>> cachedOutRDD = outRDD;
            this.getCurrentFlow().addCleaner(new Cleaner() {
                @Override public void doSuccess() {
                    cachedOutRDD.unpersist(false);
                }
            });
        }

        // 调试
//        List l = outRDD.collect();
        return outRDD;
    }

    public List<TTransformationAction> gettTransformationActions() {
        return tTransformationActions;
    }

    public void settTransformationActions(List<TTransformationAction> tTransformationActions) {
        this.tTransformationActions = tTransformationActions;
    }

    public TSquid getPreviousSquid() {
        return previousSquid;
    }

    public void setPreviousSquid(TSquid previousSquid) {
        this.previousSquid = previousSquid;
    }

}
