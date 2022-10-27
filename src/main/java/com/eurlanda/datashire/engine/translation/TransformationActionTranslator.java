package com.eurlanda.datashire.engine.translation;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.engine.entity.TFilterExpression;
import com.eurlanda.datashire.engine.entity.TTransformation;
import com.eurlanda.datashire.engine.entity.TTransformationAction;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.engine.util.StringUtils;
import com.eurlanda.datashire.engine.util.cool.JList;
import com.eurlanda.datashire.engine.util.cool.JMap;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.DSVariable;
import com.eurlanda.datashire.entity.DataSquid;
import com.eurlanda.datashire.entity.ReferenceColumn;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.entity.TransformationInputs;
import com.eurlanda.datashire.entity.TransformationLink;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.*;
import java.util.regex.Matcher;

public abstract class TransformationActionTranslator {

    protected static Log log = LogFactory.getLog(TransformationActionTranslator.class);

    protected DataSquid dataSquid;
    protected boolean hasException = false;
    protected Map<String, DSVariable> variableMap;
    protected Map<Integer, Integer> squidDropMapper = new HashMap();
    protected Map<Integer, TTransformation> trsCache = new JMap<>();
    protected IdKeyGenerator idKeyGenerator;

    protected List<TransformationLink> preparedLinks = new ArrayList<>();

    public TransformationActionTranslator(DataSquid dataSquid, IdKeyGenerator idKeyGenerator, Map<String, DSVariable> variableMap) {
        this.dataSquid = dataSquid;
        this.idKeyGenerator = idKeyGenerator;
        this.variableMap = variableMap;
    }

    public List<TTransformationAction> translateTTransformationActions() {

        this.prepareLinks();
        // 判断该stageSquid是否有exceptionSquid连接
        if (hasException) {

            while (this.cutTrans() > 0) {
                // continue cut
            }
        } else {
            while (this.cutTransWithNoExceptionSquid() > 0) {
                // continue cut
            }
        }

        List<TTransformationAction> actions = buildTransformationActions();
//        ts.settTransformationActions(actions);
        return actions;
    }

    protected abstract List<TTransformationAction> buildTransformationActions();

    protected void prepareLinks() {
        List<Transformation> trans = dataSquid.getTransformations();
        this.preparedLinks.addAll(dataSquid.getTransformationLinks());
        //referenceColumn的transformations
        List<Transformation> begins = this.getBeginTrs();
        if (begins.size() == 0) return;
        int firstTrsId = begins.get(0).getId();
        for (Transformation tf : trans) {
            prepareVariableLinks(tf);
            prepareConstantTrs(tf, firstTrsId);
            //判断当前transformation是否是常量和null，如果是的话，处理逻辑和常量一样，手动设置一个入参
            prepareConstantInputs(tf,firstTrsId);
        }

    }

    /**
     * 对transformation进行剪枝操作
     */
    private int cutTrans() {
        List<Transformation> trans = dataSquid.getTransformations();
        List<Transformation> removes = new ArrayList<>();
        for (Transformation tf : trans) {
            if (tf.getTranstype() == TransformationTypeEnum.VIRTUAL.value()) {
                continue;
            }
            // 找到是否有link从该transformation连出，
            // 1.有则说明这个transformation存在下游导出，
            // 2.没有则需要将该transformation删除，并且删除所有连入到该transformation的link
            List<TransformationLink> fromlinks = getLinksFromTran(tf);
            if (fromlinks.size() == 0) {
                // 删除所有连入到该transformation的link
                List<TransformationLink> tolinks = getLinksToTran(tf);
                Iterator<TransformationLink> links = this.preparedLinks.iterator();
                if(tolinks!=null && tolinks.size()>0) {
                    while (links.hasNext()) {
                        TransformationLink link = links.next();
                        for(TransformationLink toLink : tolinks){
                            if (link.getFrom_transformation_id() == toLink.getFrom_transformation_id()
                                    && link.getTo_transformation_id() == toLink.getTo_transformation_id()){
                                links.remove();
                            }
                        }
                    }
                }
                //this.preparedLinks.removeAll(tolinks);
                // 删除该transformation
                removes.add(tf);
            }
        }
        try { //  关联规则的 train 没有连接到任何一列，train 不能删除
            SquidTypeEnum squidType = SquidTypeEnum.valueOf(this.dataSquid.getSquid_type());
            if (squidType == SquidTypeEnum.ASSOCIATION_RULES) {
               PersistAssociationRulesTrans(removes);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        trans.removeAll(removes);
        return removes.size();
    }

    protected List<Transformation> getBeginTrs() {
        List<Transformation> ret = new ArrayList<>();
        for (ReferenceColumn col : dataSquid.getSourceColumns()) {
            for (Transformation trs : dataSquid.getTransformations()) {
                if (trs.getColumn_id() == col.getColumn_id()) {
                    ret.add(trs);
                }
            }
        }
        return ret;
    }

    private void prepareVariableLinks(Transformation tf) {
        ArrayList<String> condList = new ArrayList<>();
        if (tf.getTranstype() == TransformationTypeEnum.CHOICE.value()) {
            for (TransformationInputs input : tf.getInputs()) {
                condList.add(input.getIn_condition());
            }
        }
        condList.add(tf.getTran_condition());
        for (String cond : condList) {
            if (!ValidateUtils.isEmpty(cond)) {
                Matcher
                        nameMatcher =
                        StringUtils.match(cond, "#([^>=<\\s]*)");        // transformation

                while (nameMatcher.find()) {
                    String tName = nameMatcher.group(1).trim();
                    TransformationLink link = new TransformationLink();
                    link.setFrom_transformation_id(getTranByName(tName).getId());
                    link.setTo_transformation_id(tf.getId());
                    preparedLinks.add(link);
                }
                nameMatcher =
                        StringUtils.match(cond, "\\.([^>=<\\s]*)");        // referenceColumn
                while (nameMatcher.find()) {
                    String tName = nameMatcher.group(1).trim();
                    if (ValidateUtils.isNumeric(tName))
                        continue;        // 数字的1.0会与squid.col 冲突。
                    TransformationLink link = new TransformationLink();
                    link.setFrom_transformation_id(getTranByColumnName(tName).getId());
                    link.setTo_transformation_id(tf.getId());
                    preparedLinks.add(link);
                }
            }
        }

    }

    /**
     * 按名字找到transformation
     *
     * @param name
     * @return
     */
    private Transformation getTranByName(String name) {
        for (Transformation tf : dataSquid.getTransformations()) {
            if (name.equals(tf.getName())) {
                return tf;
            }
        }
        return null;

    }

    /**
     * 按照olumnName匹配 virtual Transformation.
     *
     * @param colName referenceColumn id或者 column id.
     * @return
     */
    private Transformation getTranByColumnName(String colName) {

        for (ReferenceColumn rc : dataSquid.getSourceColumns()) {
            if (colName.equals(rc.getName())) {
                return getTranByColumnId(rc.getColumn_id());
            }
        }
        return null;
    }

    /**
     * 按照olumnId匹配 virtual Transformation.
     *
     * @param colId referenceColumn id或者 column id.
     * @return
     */
    protected Transformation getTranByColumnId(Integer colId) {
        for (Transformation tf : dataSquid.getTransformations()) {
            if (colId.equals(tf.getColumn_id())) {
                return tf;
            }
        }
        return null;
    }

    private List<TransformationLink> getLinksFromTran(Transformation transformation) {
        List<TransformationLink> links = new ArrayList<>();
        for (TransformationLink tl : this.preparedLinks) {
            if (tl.getFrom_transformation_id() == transformation.getId()) {
                links.add(tl);
            }
        }
        return links;
    }

    private List<TransformationLink> getLinksToTran(Transformation transformation) {
        List<TransformationLink> links = new ArrayList<>();
        for (TransformationLink tl : this.preparedLinks) {
            if (tl.getTo_transformation_id() == transformation.getId()) {
                links.add(tl);
            }
        }
        return links;
    }

    /**
     * 没有连接exceptionSquid时，对transformation进行剪枝操作
     */
    private int cutTransWithNoExceptionSquid() {
        List<Transformation> trans = dataSquid.getTransformations();
        List<Transformation> removes = new ArrayList<>();
        Column idColumn = null;
        for (Column c : dataSquid.getColumns()) {
            if ("id".equals(c.getName())) {
                idColumn = c;
            }
        }
        for (Transformation tf : trans) {
            if (tf.getTranstype() == TransformationTypeEnum.VIRTUAL.value()) {
                List<TransformationLink> toLinks = getLinksToTran(tf);
                if (toLinks.size() == 0) {
                    List<TransformationLink> fromLinks = getLinksFromTran(tf);
                    if (fromLinks.size() == 0) {
                        // 排除ID COLUMN的列
                        if (idColumn != null && tf.getColumn_id() != idColumn.getId()) {
                            removes.add(tf);
                        }
                    }
                }
                continue;
            }
            // 找到是否有link从该transformation连出，
            // 1.有则说明这个transformation存在下游导出，
            // 2.没有则需要将该transformation删除，并且删除所有连入到该transformation的link
            List<TransformationLink> fromlinks = getLinksFromTran(tf);
            if (fromlinks.size() == 0) {
                // 删除所有连入到该transformation的link
                List<TransformationLink> tolinks = getLinksToTran(tf);
                Iterator<TransformationLink> links = this.preparedLinks.iterator();
                if(tolinks!=null && tolinks.size()>0) {
                    while (links.hasNext()) {
                        TransformationLink link = links.next();
                        for(TransformationLink toLink : tolinks){
                            if (link.getFrom_transformation_id() == toLink.getFrom_transformation_id()
                                    && link.getTo_transformation_id() == toLink.getTo_transformation_id()){
                                links.remove();
                            }
                        }
                    }
                }
                //this.preparedLinks.removeAll(tolinks);
                // 删除该transformation
                removes.add(tf);
            }
        }
        //  trans.removeAll(removes);
        try { //  关联规则的 train 没有连接到任何一列，train 不能删除
            SquidTypeEnum squidType = SquidTypeEnum.valueOf(this.dataSquid.getSquid_type());
            if (squidType == SquidTypeEnum.ASSOCIATION_RULES) {
                PersistAssociationRulesTrans(removes);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        trans.removeAll(removes);
        return removes.size();
    }

    /**
     * 依据表达式创建虚拟link.
     *
     * @param tf
     * @return
     */
    private void prepareConstantTrs(Transformation tf, int firstTrsId) {
        if (isConstantTrs(tf)) {
            TransformationLink link = new TransformationLink();
            link.setFrom_transformation_id(firstTrsId);
            link.setTo_transformation_id(tf.getId());
            preparedLinks.add(link);
        }
    }

    /**
     * 判断该trans的transinput是否是手动输入的，如果是，手动创建一个入参
     * @param trs
     * @return
     */
    protected  void prepareConstantInputs(Transformation trs,int firstTrsId){
        TransformationTypeEnum typeEnum = TransformationTypeEnum.parse(trs.getTranstype());
        if(typeEnum != TransformationTypeEnum.VIRTUAL){
            //获取到transinputs
            List<TransformationInputs> inputs = trs.getInputs();
            if(inputs != null && inputs.size()>0){
                for(TransformationInputs input : inputs){
                    int sourceType = input.getSource_type();
                    if(sourceType == 1 || sourceType == 2){
                        TransformationLink link = new TransformationLink();
                        link.setFrom_transformation_id(firstTrsId);
                        link.setTo_transformation_id(trs.getId());
                        preparedLinks.add(link);
                    }
                }
            }
        }
    }

    /**
     * 判断transformation 是否是常量。有显示常量和隐式常量（如随机数，当前日期）。
     *
     * @param trs
     * @return
     */
    protected boolean isConstantTrs(Transformation trs) {
        TransformationTypeEnum typeEnum = TransformationTypeEnum.parse(trs.getTranstype());
        switch (typeEnum) {
        case CONSTANT:
        case RANDOM:
        case SYSTEMDATETIME:
        case PI:
        case JOBID:
        case PROJECTID:
        case PROJECTNAME:
        case SQUIDFLOWID:
        case SQUIDFLOWNAME:
        case TASKID:
        case UUID:
            return true;
        default:
            return false;
        }
    }


    /**
     * 取指定trs的输出Trans
     *
     * @param tf
     * @return
     */
    protected List<Transformation> getOutTrs(Transformation tf) {
        List<Transformation> ret = new ArrayList<>();
        for (TransformationLink link : preparedLinks) {
            if (link.getFrom_transformation_id() == tf.getId()) {
                Transformation outTrs = this.getTranById(link.getTo_transformation_id());
                ret.add(outTrs);
            }
        }
        return ret;
    }

    protected Transformation getTranById(int id) {
        for (Transformation tf : dataSquid.getTransformations()) {
            if (tf.getId() == id) {
                return tf;
            }
        }
        return null;
    }

    /**
     * 取JdbcTemplate.
     * @return
     */
    public JdbcTemplate getCurJdbc(){
        return ConstantUtil.getJdbcTemplate();
    }

    /**
     * 抛弃transformation.并且返回可用的抛弃值。
     *
     * @param tf
     * @return
     */
    protected HashSet<Integer> getDropedTrsKeys(Transformation tf) {
        List<Transformation> ins = this.getInTrs(tf);
        HashSet<Integer> ret = new HashSet<>();
        if (isFirstTrsOfSquid(tf)) { // 如果是第一个，那么移除column key.
            ret.add(tf.getColumn_id());
        } else {
            for (Transformation in : ins) {

                Integer outSize = this.getOutTrs(in).size();

                Integer dropCount = squidDropMapper.get(in.getId());

                dropCount = dropCount == null ? 1 : dropCount + 1; // 每次抛弃后加1次抛弃值。

                if (dropCount >= outSize) { // 抛弃值》= 输出值 时，此transformation 寿命已到。
                    List<Integer> outKeys = getOutKeyListByTrs(in);
                    if (outKeys != null) {
                        for (Integer x : outKeys) {
                            if (x > 0) {
                                ret.add(x);
                            }
                        }
                    }
                }

                squidDropMapper.put(in.getId(), dropCount);

            }
        }
        return ret;
    }

    /**
     * 取transformation的outKey.
     *
     * @param tf
     * @return
     */
    protected List<Integer> getOutKeyListByTrs(Transformation tf) {
        return this.trsCache.get(tf.getId()).getOutKeyList();
    }

    /**
     * 取Transformation的inkeyList.
     * trans现在修改成根据source_type来区分inputs类型
     * source_type为0 和原来的处理逻辑一样
     * source_type为1 手动输入
     * source_type为2 null值
     * @param tf
     * @return
     */
    protected ArrayList<Integer> getInkeyList(Transformation tf) {
        ArrayList<Integer> inkeys = JList.create();
        if (isFirstTrsOfSquid(tf)) {
            inkeys.add(tf.getColumn_id()); // 如果是第一个transformation,inkey是refcol的ID
        } else {
            if(TransformationTypeEnum.VIRTUAL.value()==tf.getTranstype() && tf.getInputs()==null){
                throw new RuntimeException("column id ["+tf.getColumn_id()+"]对应的transformation inputs 为空");
            }
           if(tf.getTranstype() == TransformationTypeEnum.RULESQUERY.value() ) { // RULESQUERY的inputs可以输入部分
                for (TransformationInputs input : tf.getInputs()) {
                    //id为0
                    if(input.getSource_type() == 1 || input.getSource_type() == 2){
                        inkeys.add(0);
                        continue;
                    }
                    Integer trsId = input.getSource_transform_id();
                    if (trsId == null || trsId.equals(0)) {
                        inkeys.add(-1); //没有连线输入
                        continue;
                    }
                    Integer idx = input.getSource_tran_output_index();
                    TTransformation ttf = this.trsCache.get(trsId);
                    inkeys.add(ttf.getOutKeyList().get(idx)); // 按照inputs指定上游的Trans
                }
            }else {
                for (TransformationInputs input : tf.getInputs()) {
                    if(input.getSource_type() == 1 || input.getSource_type() == 2){
                        inkeys.add(0);
                        continue;
                    }
                    Integer trsId = input.getSource_transform_id();
                    if (trsId == null || trsId == 0) {
                      //  throw new RuntimeException("transformationInput[" + input.getId() + "]的source_transform_id为空,tran_id[" + tf.getId() + "]");   
                        continue; 
                    }
                    Integer idx = input.getSource_tran_output_index();
                    TTransformation ttf = this.trsCache.get(trsId);
                    inkeys.add(ttf.getOutKeyList().get(idx)); // 按照inputs指定上游的Trans。
                }
            }
        }
        return inkeys;
    }

    /**
     * 取TRS的filter。
     *
     * @param tf
     * @return
     */
    protected List<TFilterExpression> getFilterList(Transformation tf, Squid squid) {
        List<TFilterExpression> filterLists = new ArrayList<>();
        TransformationTypeEnum tfType = TransformationTypeEnum.parse(tf.getTranstype());
        if (tf.getInputs() != null && tf.getInputs().size() > 0) {
            for (TransformationInputs input : tf.getInputs()) {
                if (tfType.equals(TransformationTypeEnum.CHOICE)) { // 如果是choice
                    //					filterLists.add(translateTrsFilter(input.getIn_condition(), squid));
                    filterLists.add(TranslateUtil.translateTransformationFilter(input.getIn_condition(), (DataSquid)squid, this.trsCache,
                            variableMap));
                }
            }
        }
        return filterLists;
    }

    /**
     * 取指定trs的输入Trans
     *
     * @param tf
     * @return
     */
    protected List<Transformation> getInTrs(Transformation tf) {
        List<Transformation> ret = new ArrayList<>();
        for (TransformationLink link : preparedLinks) {
            if (link.getTo_transformation_id() == tf.getId()) {
                Transformation outTrs = this.getTranById(link.getFrom_transformation_id());
                ret.add(outTrs);
            }
        }
        return ret;
    }

    /**
     * 生成Transformation的outKeyList.
     * 默认生成一个outKey,无论有没有线连到它。
     *
     * @param tf
     * @return
     */
    protected ArrayList<Integer> genOutKeyList(Transformation tf) {
        int outputs = tf.getOutput_number();
        outputs = outputs > 0 ? outputs : 1;
        ArrayList<Integer> outKeys = JList.create();
        if (isLastTrsOfSquid(tf)) {
            outKeys.add(tf.getColumn_id());
        } else if (isFirstTrsOfSquid(tf)) {
            outKeys.add(-tf.getColumn_id());
        } else {
            for (int i = 0; i < outputs; i++) {
                outKeys.add(idKeyGenerator.genKey());
            }
        }
        return outKeys;
    }

    /**
     * 判断一个transformation是否是最后一个虚拟转换。
     *
     * @param tf
     * @return
     */
    protected boolean isLastTrsOfSquid(Transformation tf) {
        for (Column col : dataSquid.getColumns()) {
            if (col.getId() == tf.getColumn_id()) {
                return true;
            }
        }
        return false;
    }

    /**
     * 判断一个transformation是否是第一个虚拟转换。
     *
     * @param tf
     * @return
     */
    protected boolean isFirstTrsOfSquid(Transformation tf) {
        for (ReferenceColumn col : dataSquid.getSourceColumns()) {
            if (col.getColumn_id() == tf.getColumn_id()) {
                return true;
            }
        }
        return false;
    }

    /**
     *  关联规则的 train 没有连接到任何一列，train 不能删除
     * @param removes
     */
    private void PersistAssociationRulesTrans(List<Transformation> removes) {
        for (TransformationLink link : dataSquid.getTransformationLinks()) {
            for (Transformation transformation : dataSquid.getTransformations()) {
                if (link.getFrom_transformation_id() == transformation.getId() ||
                        link.getTo_transformation_id() == transformation.getId()) {
                    removes.remove(transformation); //保留的trans
                    if (!this.preparedLinks.contains(link)) {
                        this.preparedLinks.add(link);
                    }
                }
            }
        }
    }

}