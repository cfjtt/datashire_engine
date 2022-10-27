package com.eurlanda.datashire.engine.translation;

import cn.com.jsoft.jframe.utils.ClassUtils;
import com.eurlanda.datashire.common.util.HbaseUtil;
import com.eurlanda.datashire.engine.entity.ESquid;
import com.eurlanda.datashire.engine.entity.TTransformation;
import com.eurlanda.datashire.engine.entity.TTransformationAction;
import com.eurlanda.datashire.engine.enumeration.TTransformationInfoType;
import com.eurlanda.datashire.engine.spark.stream.SSquid;
import com.eurlanda.datashire.engine.spark.stream.STransformationSquid;
import com.eurlanda.datashire.engine.spark.translation.StreamBuilderContext;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.DSVariable;
import com.eurlanda.datashire.entity.DataMiningSquid;
import com.eurlanda.datashire.entity.DataSquid;
import com.eurlanda.datashire.entity.ReferenceColumn;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.utility.EnumException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Created by zhudebin on 16/3/17.
 */
public class StreamStageTransformationActionTranslator extends TransformationActionTranslator {

    private StreamBuilderContext sbc;
    private STransformationSquid sTransformationSquid;

    public StreamStageTransformationActionTranslator(
            DataSquid dataSquid, IdKeyGenerator idKeyGenerator, Map<String, DSVariable> variableMap, StreamBuilderContext sbc, STransformationSquid sTransformationSquid) {
        super(dataSquid, idKeyGenerator, variableMap);
        this.sbc = sbc;
        this.sTransformationSquid = sTransformationSquid;
    }

    @Override protected List<TTransformationAction> buildTransformationActions() {
        List<TTransformationAction> actions = new ArrayList<>();
        List<Transformation> begins = this.getBeginTrs();

        // 添加一个对没有参与转换的virtual transformation的key进行remove
        List<ReferenceColumn> referenceColumns = dataSquid.getSourceColumns();
        HashSet<Integer> removeKeys = new HashSet<>();
        for(ReferenceColumn rc : referenceColumns) {
            removeKeys.add(rc.getColumn_id());
            for(Transformation t : begins) {
                if(t.getColumn_id() == rc.getColumn_id()) {
                    removeKeys.remove(rc.getColumn_id());
                    break;
                }
            }
        }
        boolean isRemoved = true;
        if(removeKeys.size()>0) {
            isRemoved = false;
        }

        Queue<Transformation> workQue = new LinkedList();
        workQue.addAll(begins);
        int loopCount = 0;
        while (workQue.size() > 0) {
            Transformation tf = workQue.poll();
            TTransformationAction action = this.buildAction(sTransformationSquid,tf, dataSquid);
            if(action==null){		// 如果当前squid不能被编译，那么等待本轮编译完成之后再编译。
                workQue.add(tf);
                loopCount++;
                if(loopCount > workQue.size()) {
                    // 打印存在问题的transformation
                    StringBuilder sb = new StringBuilder("{");
                    while(workQue.size()>0) {
                        Transformation t = workQue.poll();
                        sb.append("[").append(t.getName()).append("] ");
                    }
                    sb.append("}");
                    throw new RuntimeException("翻译transformation异常,存在异常引用transformatons " + sb.toString());
                } else {
                    continue;
                }
            } else {
                loopCount = 0;
            }
            if(!isRemoved) {
                Set<Integer> rmKeys = action.getRmKeys();
                if(rmKeys == null) {
                    action.setRmKeys(removeKeys);
                } else {
                    rmKeys.addAll(removeKeys);
                }
            }
            // 判断是否为常量transformation
            TTransformation ttran = action.gettTransformation();
            TranslateUtil.transVariable(ttran, variableMap, dataSquid);

            /**
             * 增加变量，及squid_id
             */
            action.gettTransformation().getInfoMap().put(TTransformationInfoType.VARIABLE.dbValue, variableMap);
            action.gettTransformation().getInfoMap().put(TTransformationInfoType.SQUID_ID.dbValue,
                    dataSquid.getId());

            actions.add(action);
            List<Transformation> outs = this.getOutTrs(tf);

            if (outs != null) { // 添加子节点
                for(Transformation x: outs){
                    if(!workQue.contains(x)){
                        workQue.add(x);
                    }
                }
            }
        }
        return actions;
    }

    /**
     * 待完成
     *
     * @param tf
     * @return
     */
    protected TTransformationAction buildAction(SSquid sSquid,Transformation tf, Squid cursquid) {

        //检查本transformation是否满足运行条件。
        List<Transformation> intrs = this.getInTrs(tf);
        for(Transformation x: intrs){
            if(!trsCache.containsKey(x.getId())){
                log.debug("transformation不满足编译条件，退出。");
                return null;
            }
        }

        TTransformationAction action = new TTransformationAction();
        TTransformation ttf = new TTransformation();
        // todo: 需要确定每个transformation 的参数。
        // ttf.putInfo(tf.get);
        TransformationTypeEnum typeEnum = null;
        try {
            typeEnum = TransformationTypeEnum.valueOf(tf.getTranstype());
            ttf.setType(typeEnum);
        } catch (EnumException e) {
            e.printStackTrace();
        }
        ttf.setOutKeyList(genOutKeyList(tf));

        //		ttf.setFilterExpression(translateTrsFilter(tf.getTran_condition(), cursquid));
        ttf.setFilterExpression(TranslateUtil.translateTransformationFilter(tf.getTran_condition(), (DataSquid) cursquid, this.trsCache, variableMap));
        ttf.setInputsFilter(getFilterList(tf, cursquid));
        if(!isConstantTrs(tf)){
            ttf.setInKeyList(getInkeyList(tf));
        }
        ttf.setName(tf.getName());
        ttf.setDbId(tf.getId());
        HashMap<String, Object> infoMap = (HashMap<String, Object>) ClassUtils.bean2Map(tf);
        // 转换变量的值
        TranslateUtil.convertVariable(infoMap, variableMap, cursquid);
        infoMap.put(TTransformationInfoType.SQUID_FLOW_ID.dbValue, sbc.squidFlow().getId());
        infoMap.put(TTransformationInfoType.SQUID_FLOW_NAME.dbValue, sbc.squidFlow().getName());
        infoMap.put(TTransformationInfoType.PROJECT_ID.dbValue, sbc.squidFlow().getProject_id());
        infoMap.put(TTransformationInfoType.PROJECT_NAME.dbValue,sbc.project().getName());
        infoMap.put(TTransformationInfoType.TASK_ID.dbValue,sbc.taskId());
        infoMap.put(TTransformationInfoType.JOB_ID.dbValue, sbc.jobId());

        // 对infoMap中可以出现变量的值进行转换


        if(dataSquid instanceof DataMiningSquid && typeEnum.equals(TransformationTypeEnum.VIRTUAL)){
            infoMap.put(TTransformationInfoType.SKIP_VALIDATE.dbValue, true);
        }
        if (tf.getDictionary_squid_id() != 0) {
            Integer columnId = 0;
            Squid squid = this.sbc.getSquidById(tf.getDictionary_squid_id());
            if (squid instanceof DataSquid) {
                columnId = ((DataSquid) squid).getColumns().get(0).getId();
            }

            ESquid ts = this.sbc.getSquidOut(squid.getId());
            infoMap.put(TTransformationInfoType.DICT_SQUID.dbValue, ts);
            infoMap.put(TTransformationInfoType.DICT_COLUMN_KEY.dbValue, columnId);
        }
        // 训练数据
        if(typeEnum.equals(TransformationTypeEnum.TRAIN)){
            infoMap.put(TTransformationInfoType.PREDICT_MODEL_TYPE.dbValue, SquidTypeEnum.parse(this.dataSquid.getSquid_type()));

        }else if(typeEnum.equals(TransformationTypeEnum.PREDICT) ||
                typeEnum.equals(TransformationTypeEnum.INVERSEQUANTIFY) ){		// 预测模型。
            int predictSquidId = tf.getModel_squid_id();
            // 拿到预测模型。
            Map<String,Object> dm_squid=getCurJdbc().queryForMap("select s.* from ds_squid s   where s.id=?",predictSquidId);
          //  Map<String,Object> dm_squid=getCurJdbc().queryForMap("select dm.*,s.* from datashire_dataMining dm inner join ds_squid s on s.id=dm.id  where s.id=?",predictSquidId);

            //	Integer isVersion = (Integer) dm_squid.get("VERSIONING");
            Integer secVersion = tf.getModel_version();
            Integer secSFID = (Integer) dm_squid.get("SQUID_FLOW_ID");
           // 从 Hbase 获取训练完毕的模型
           String sql = "select model from "+ HbaseUtil.genTrainModelTableName(this.sbc.project().getRepository_id(), secSFID, predictSquidId);

            // 判断是否指定key
            boolean isKey = false;
            if(ttf.getInKeyList().size()==1) {
                isKey = false;
            } else if(ttf.getInKeyList().size()==2) {
                isKey = true;
                sql += " where \"KEY\" = ? ";
            }
            if(secVersion== -1){
                sql+=" order by version desc limit 1";
            }else{
                if(isKey) {
                    sql += " and version =" + secVersion;
                } else {
                    sql += " where version =" + secVersion;
                }
            }
            // 模型的数据查询迁移到运行时，
            //			Map<String,Object> data = ConstantUtil.getHbaseJdbc().queryForMap(sql);
            infoMap.put(TTransformationInfoType.PREDICT_MODEL_TYPE.dbValue, SquidTypeEnum.parse((Integer) dm_squid.get("SQUID_TYPE_ID")));
            // 先放SQL
            infoMap.put(TTransformationInfoType.PREDICT_DM_MODEL.dbValue,sql);
            // 如果预测使用了其它squid的产生的模型并且该squid与依赖的squid在同一squid，必须设置依赖.
            if(secSFID==this.sbc.squidFlow().getId()){
                ESquid out = this.sbc.getSquidOut(predictSquidId);
                sSquid.addDependenceSquid(out);
            }
        }
        if (isLastTrsOfSquid(tf)) { // column 指定notnull
            int colId = tf.getColumn_id();
            for (Column col : dataSquid.getColumns()) {
                if (col.getId() == colId) {
                    boolean bool = col.isNullable();
                    infoMap.put(TTransformationInfoType.IS_NULLABLE.dbValue, bool);
                    infoMap.put(TTransformationInfoType.STRING_MAX_LENGTH.dbValue, col.getLength());
                    infoMap.put(TTransformationInfoType.NUMERIC_PRECISION.dbValue, col.getPrecision());
                    infoMap.put(TTransformationInfoType.NUMERIC_SCALE.dbValue, col.getScale());
                    // 放置的系统类型，主要用于对于smallInt, bigInt,tinyInt等进行数据大小验证
                    if(col.getAggregation_type()<=0) {
                        infoMap.put(TTransformationInfoType.VIRT_TRANS_OUT_DATA_TYPE.dbValue, col.getData_type());
                    } else {
                        // 标记这个transformation为 参与聚合的列，不需要对数据进行校验
                        if(!col.isIs_groupby()) {
                            infoMap.put(TTransformationInfoType.AGGREGATION_COLUMN.dbValue, 1);
                        }
                    }
                    break;
                }
            }
        }
        ttf.setInfoMap(infoMap);

        action.settTransformation(ttf);

        action.setRmKeys(getDropedTrsKeys(tf));

        trsCache.put(tf.getId(), ttf);

        return action;
    }
}
