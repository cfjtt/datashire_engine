package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.engine.dao.TransformationInputsDao;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.engine.util.StringUtils;
import com.eurlanda.datashire.entity.DataMiningSquid;
import com.eurlanda.datashire.entity.DataSquid;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.SquidFlow;
import com.eurlanda.datashire.entity.StageSquid;
import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.entity.TransformationInputs;
import com.eurlanda.datashire.entity.TransformationLink;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.utility.EnumException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * Created by zhangwen on 2016/4/18.
 */
public class TransformationValidator {

    private static final TransformationInputsDao transformationInputsDao = ConstantUtil.getTransformationInputsDao();

    private SquidFlow squidFlow;


    public TransformationValidator(SquidFlow squidFlow) {
        this.squidFlow = squidFlow;
    }

    public Map<String, String> validate() {
        Map<String, String> messages = validateTransformationOutput();
            return messages;


    }
    public Map<String, String> validateTransformationOutput(){
        ArrayList<Squid> squidList = (ArrayList<Squid>) squidFlow.getSquidList();
        for (Squid squid:squidList){
            if(squid instanceof StageSquid||squid instanceof DataMiningSquid){
                DataSquid dataSquid = (DataSquid) squid;
                List<Transformation> trans = dataSquid.getTransformations();
                for(Transformation tran:trans){
                    if(tran.getTranstype()!=0) {
                        //transformation输出
                        List<Transformation> realOutTransformations = getOutTrs(tran, dataSquid.getTransformationLinks(), dataSquid);
                        if (realOutTransformations != null && realOutTransformations.size() == 0) {
                          return makeMessages(dataSquid,squidFlow,tran,"","","转换输出数量异常");
                        }
                        //验证transformation输出类型
                        if (realOutTransformations != null && realOutTransformations.size() != 0) {
                            if (tran.getTranstype() != TransformationTypeEnum.CALCULATION.value()) {
                                for (Transformation transformation : realOutTransformations) {
                                    if (transformation.getOutput_data_type() != tran.getOutput_data_type()) {
                                        return makeMessages(dataSquid,squidFlow,tran,"","","转换输出类型异常");
                                    }
                                }
                            } else {
                                for (Transformation transformation : realOutTransformations) {
                                    SystemDatatype transDataTypeEnum = SystemDatatype.parse(transformation.getOutput_data_type());
                                    if (transDataTypeEnum == SystemDatatype.BIGINT || transDataTypeEnum == SystemDatatype.DECIMAL ||
                                            transDataTypeEnum == SystemDatatype.FLOAT || transDataTypeEnum == SystemDatatype.INT ||
                                            transDataTypeEnum == SystemDatatype.SMALLINT ||
                                            transDataTypeEnum == SystemDatatype.TINYINT || transDataTypeEnum == SystemDatatype.BIT) {

                                    } else {
                                        return makeMessages(dataSquid,squidFlow,tran,"","","转换输出类型异常");
                                    }
                                }
                            }

                        }

                        //transformation输入
                        List<TransformationInputs> targetTransformations = tran.getInputs();
                        List<Transformation> realTransformations = getInTrs(tran, dataSquid.getTransformationLinks(), dataSquid);
                        if (targetTransformations != null && realTransformations != null && realTransformations.size() != 0 && targetTransformations.size() != realTransformations.size()) {
                            return makeMessages(dataSquid,squidFlow,tran,"","","转换输入数量异常");
                        }
                        //transformation 类型匹配
                        List<Integer> inPuts = null;
                        try {
                            inPuts = transformationInputsDao.getTargetInputType(tran.getTranstype());
                        } catch (EnumException e) {
                            e.printStackTrace();
                        }
                        List<Integer> realInputs = new ArrayList<>();

                        for (Transformation realTran : realTransformations) {
                            realInputs.add(realTran.getOutput_data_type());
                        }

                        if (tran.getTranstype() != TransformationTypeEnum.CALCULATION.value()) {

                            if (!isArrayListSameWithoutOrder(inPuts, realInputs)) {
                                return makeMessages(dataSquid,squidFlow,tran,"","","转换输入类型异常");
                            }
                        } else {
                            for (Transformation tranreal : realTransformations) {
                                SystemDatatype transDataTypeEnum = SystemDatatype.parse(tranreal.getOutput_data_type());
                                if (transDataTypeEnum == SystemDatatype.BIGINT || transDataTypeEnum == SystemDatatype.DECIMAL ||
                                        transDataTypeEnum == SystemDatatype.FLOAT || transDataTypeEnum == SystemDatatype.INT ||
                                        transDataTypeEnum == SystemDatatype.SMALLINT ||
                                        transDataTypeEnum == SystemDatatype.TINYINT || transDataTypeEnum == SystemDatatype.BIT) {

                                } else {
                                    return makeMessages(dataSquid,squidFlow,tran,"","","转换输入类型异常");
                                }
                            }

                        }

                        //依赖检查
                    if(tran.getTran_condition()!=null&&tran.getTran_condition().length()!=0){
                        ArrayList<String> upstreamTransName = new ArrayList<>();
                        for (Transformation realTran : realTransformations) {
                            if(realTran.getTranstype()!=0&& org.apache.commons.lang3.StringUtils.isNoneEmpty(realTran.getName())) {
                                upstreamTransName.add(realTran.getName());
                                List<Transformation> upTrans = getInTrs(realTran, dataSquid.getTransformationLinks(), dataSquid);
                                putTransName(upTrans, upstreamTransName, dataSquid);
                            }
                        }
                        List<String> inputVars = extractNameFromCondition(tran.getTran_condition());
                        if(inputVars==null||inputVars.size()==0){
                            return makeMessages(dataSquid,squidFlow,tran,"","","条件输入表达式错误");
                        }

                            for (String var : inputVars) {
                                if (upstreamTransName.contains(var)) {

                                } else {
                                    return makeMessages(dataSquid,squidFlow,tran,"","","条件输入不能依赖下游转换");
                                }
                            }

                    }

                    }
                }
            }
        }
        return new HashMap<String,String>();

    }
    public  Map<String,String> makeMessages(Squid squid, SquidFlow squidFlow, Transformation tran,String messageSquidFlow,String messageSquid,String messageTransformation){
        Map<String, String> messages = new HashMap<String, String>();
        messages.put("squidflow","id:"+squidFlow.getId()+",message:\""+messageSquidFlow+"\"");
        messages.put("squids","[{id:"+squid.getId()+",message:\""+messageSquid+"\"}]");
        messages.put("transformations","[{squidId:"+"squid.getId()"+"id:"+tran.getId()+",message:\""+messageTransformation+"\"}]");
        return messages;
    }

    protected List<String> extractNameFromCondition(String filterString){
        ArrayList<String> list = new ArrayList<>();
        String fs = filterString.replaceAll("(?<=[^><=!])=(?=[^><=])", " == ")
                .replaceAll("\\s(?i)and", " && ")
                .replaceAll("\\s(?i)or", " || ")
                .replaceAll("#|(\\w+)\\.(?!\\d+)", "$1\\$")
                .replace("'", "\"")
                .replace("<>", "!=");

        // 建立变量映射。查找变量
        Matcher matcher = StringUtils.match(fs, "(\\w+\\$\\w+)|(\\$\\w+)");
        Map<String,Integer> keyMap = new HashMap<String, Integer>();
        while(matcher.find()){
            String var = matcher.group();
            list.add(var.substring(1));
        }
        return list;
    }
    protected void putTransName(List<Transformation> trans, ArrayList<String> upstreamTransName,DataSquid dataSquid){
        for (Transformation tran:trans){
            if(tran.getTranstype()!=0&& org.apache.commons.lang3.StringUtils.isNoneEmpty(tran.getName())) {
                upstreamTransName.add(tran.getName());
                List<Transformation> upTrans = getInTrs(tran, dataSquid.getTransformationLinks(), dataSquid);
                putTransName(upTrans, upstreamTransName, dataSquid);
            }
        }
    }
    /**
     * 取指定trs的输入Trans
     *
     * @param tf
     * @return
     */
    protected List<Transformation> getInTrs(Transformation tf,List<TransformationLink> preparedLinks,DataSquid dataSquid) {
        List<Transformation> ret = new ArrayList<>();
        for (TransformationLink link : preparedLinks) {
            if (link.getTo_transformation_id() == tf.getId()) {
                Transformation outTrs = this.getTranById(link.getFrom_transformation_id(),dataSquid);
                ret.add(outTrs);
            }
        }
        return ret;
    }
    protected Transformation getTranById(int id,DataSquid dataSquid) {
        for (Transformation tf : dataSquid.getTransformations()) {
            if (tf.getId() == id) {
                return tf;
            }
        }
        return null;
    }
    /**
     * 取指定trs的输出Trans
     *
     * @param tf
     * @return
     */
    protected List<Transformation> getOutTrs(Transformation tf,List<TransformationLink> preparedLinks,DataSquid dataSquid) {
        List<Transformation> ret = new ArrayList<>();
        for (TransformationLink link : preparedLinks) {
            if (link.getFrom_transformation_id() == tf.getId()) {
                Transformation outTrs = this.getTranById(link.getTo_transformation_id(),dataSquid);
                ret.add(outTrs);
            }
        }
        return ret;
    }
    protected boolean isArrayListSameWithoutOrder(List<Integer> left,List<Integer> right){
        if(left.size()!=right.size()){
            return false;
        }
        Collections.sort(left);
        Collections.sort(right);
        for(int i=0;i<left.size();i++){
            if(left.get(i)!=right.get(i)){
                return false;
            }
        }
        return true;
    }
}
