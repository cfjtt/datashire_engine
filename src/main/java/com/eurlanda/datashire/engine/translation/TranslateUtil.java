package com.eurlanda.datashire.engine.translation;

import cn.com.jsoft.jframe.utils.DateTimeUtils;
import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.engine.entity.*;
import com.eurlanda.datashire.engine.enumeration.TTransformationInfoType;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.exception.TargetSquidNotPersistException;
import com.eurlanda.datashire.engine.exception.TranslateException;
import com.eurlanda.datashire.engine.util.StringUtils;
import com.eurlanda.datashire.engine.util.VariableUtil;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.AggregationType;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.server.model.PivotSquid;
import com.eurlanda.datashire.utility.StringUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.types.StructField;

import java.sql.Timestamp;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zhudebin on 14-12-25.
 */
public class TranslateUtil {

    private static Log log = LogFactory.getLog(TranslateUtil.class);

    public static void transVariable(TTransformation ttran, Map<String, DSVariable> var, Squid currentSquid) {
        Map<String, Object> infoMap = ttran.getInfoMap();

        switch (ttran.getType()) {
        case CONSTANT:
            String value = (String)infoMap.get(TTransformationInfoType.CONSTANT_VALUE.dbValue);
            TDataType tDataType = TDataType.sysType2TDataType((Integer) infoMap.get(TTransformationInfoType.OUTPUT_DATA_TYPE.dbValue));
            infoMap.put(TTransformationInfoType.OUTPUT_DATA_TYPE.dbValue, tDataType);
            // 判断是否为变量
            if(StringUtils.isVariable(value)) {
                //					VariableUtil.variableValue(value.substring(1), ctx, squid);
                DSVariable dsvar = getVariable(var, value.substring(1), currentSquid);
                if(TDataType.sysType2TDataType(dsvar.getVariable_type()) == tDataType) {
                    infoMap.put(TTransformationInfoType.CONSTANT_VALUE.dbValue, VariableUtil.variable2Value(dsvar));
                } else {
                    throw new EngineException("翻译异常:常量指定的变量[" + value + "]类型与常量类型不一致");
                }
            } else {
                if(tDataType == TDataType.STRING || tDataType == TDataType.VARBINARY) {
                    value = StringUtils.scriptString(value);
                }
                infoMap.put(TTransformationInfoType.CONSTANT_VALUE.dbValue, TColumn.toTColumnValue(value, tDataType));
            }
            break;
        default:

        }
    }

    public static DataFallInfo translateDataFallInfo(DataSquid dataSquid, DbSquid dbSquid) {

        List<Column> columns = dataSquid.getColumns();
        int destinationSquidId = dataSquid.getDestination_squid_id();
        String tableName = dataSquid.getTable_name();
        int cdc = dataSquid.getCdc();

        if(dataSquid.isIs_persisted()) {
            // 默认表结构已经生成了, 表名为： sf_implied.squidFlowId + _ + squidId
            // 1. 获取所有的columns，
            // 2. 根据column信息，翻译成 TColumn
            // 3. TDataSource信息
            Set<TColumn> tColumnMap = new HashSet<>();
            //			List<Column> columns = stageSquid.getColumns();
            for (Column c : columns) {
                TColumn tColumn = new TColumn();
                tColumn.setId(c.getId());
                tColumn.setName(c.getName());
                tColumn.setLength(c.getLength());
                tColumn.setPrecision(c.getPrecision());
                tColumn.setPrimaryKey(c.isIsPK());
                tColumn.setData_type(TDataType.sysType2TDataType(c.getData_type()));
                tColumn.setNullable(c.isNullable());
                tColumn.setCdc(c.getCdc() == 1 ? 2 : 1);		//  后台的是=2，否=1
                tColumn.setBusinessKey(c.getIs_Business_Key() == 1 ? true : false);
                tColumnMap.add(tColumn);
            }

            if (destinationSquidId == 0) { // 设置落地但没有指定落地库。隐式落地。
                throw new TargetSquidNotPersistException(dataSquid.getName());
                /**
                TDataSource tDataSource = new TDataSource();
                tDataSource.setType(DataBaseType.HBASE_PHOENIX);
                // 隐式落地,改用设置的表名
                //tDataSource.setTableName(HbaseUtil.genImpliedTableName(this.ctx.getRepositoryId(), stageSquid.getSquidflow_id(), stageSquid.getId()));
                tDataSource.setTableName(tableName);
                tDataSource.setHost(ConfigurationUtil.getInnerHbaseHost() + ":" + ConfigurationUtil.getInnerHbasePort());
                //				tDataSource.setPort(ConfigurationUtil.getInnerHbasePort());
                tDataSource.setCDC(cdc == 1 ? true : false);
                // TDataSource tDataSource = new TDataSource("192.168.137.2",
                // 3306,
                // "squidflowtest", "root", "root", stageSquid.getSquidflow_id()
                // +
                // "_" + stageSquid.getId(), DataBaseType.MYSQL);
                return new DataFallInfo(tDataSource, tColumnMap);
                   */
            } else { // 指定地点的落地。
                TDataSource tDataSource = new TDataSource();
                tDataSource.setCDC(cdc == 1);
                tDataSource.setDbName(dbSquid.getDb_name());
                tDataSource.setUserName(dbSquid.getUser_name());
                tDataSource.setPassword(dbSquid.getPassword());
                tDataSource.setType(DataBaseType.parse(dbSquid.getDb_type()));
                tDataSource.setTableName(tableName);
                tDataSource.setHost(dbSquid.getHost());
                tDataSource.setPort(dbSquid.getPort());

                return new DataFallInfo(tDataSource, tColumnMap);
            }
        }
        return null;
    }

    public static class DataFallInfo {
        private TDataSource tDataSource;
        private Set<TColumn> tColumnMap;

        public DataFallInfo(TDataSource tDataSource,
                Set<TColumn> tColumnMap) {
            this.tDataSource = tDataSource;
            this.tColumnMap = tColumnMap;
        }

        public TDataSource gettDataSource() {
            return tDataSource;
        }

        public void settDataSource(TDataSource tDataSource) {
            this.tDataSource = tDataSource;
        }

        public Set<TColumn> gettColumnMap() {
            return tColumnMap;
        }

        public void settColumnMap(Set<TColumn> tColumnMap) {
            this.tColumnMap = tColumnMap;
        }
    }

    public static AggregateInfo translateAggregateInfo(DataSquid stageSquid) {
        List<Column> columns = stageSquid.getColumns();
        // group by 的列
        List<Integer> groupKeyList = new ArrayList<>();
        // 聚合操作
        List<AggregateAction> aaList = new ArrayList<AggregateAction>();
        for (int i = 0; i < columns.size(); i++) {
            Column c = columns.get(i);
            if (c == null)
                continue;
            Integer columnId = c.getId();
            AggregateAction aa = new AggregateAction();
            if (c.isIs_groupby()) {
                groupKeyList.add(c.getId());
            }

            if(c.getAggregation_type()>0) {
                aa.setType(AggregateAction.Type.parse(AggregationType.parse(c.getAggregation_type()).name()));
                if(aa.getType() == AggregateAction.Type.LAST_VALUE
                        || aa.getType() == AggregateAction.Type.FIRST_VALUE) {
                    // 查询所有的排序字段
                    aa.setOrders(genOrders(columns));
                }
            } else {
                if(c.isIs_groupby()) {
                    aa.setType(AggregateAction.Type.GROUP);
                } else if(c.getSort_level()>0) {
                    // 排序字段
                    aa.setType(AggregateAction.Type.SORT);
                } else {
                    continue;
                }
            }
            aa.setInKey(columnId);
            aa.setOutKey(columnId);
            aa.setColumn(c);//添加列
            aaList.add(aa);
        }

        if (!groupKeyList.isEmpty() || aaList.size()>0) {
            return new AggregateInfo(groupKeyList, aaList);
        }
        return null;
    }

    private static List<TOrderItem> genOrders(List<Column> columns) {
        List<TOrderItem> orders = new ArrayList<>();
        Column[] columnArray = columns.toArray(new Column[] {});
        Arrays.sort(columnArray, new Comparator<Column>() {
            @Override
            public int compare(Column o1, Column o2) {
                return o2.getSort_level() - o1.getSort_level();
            }
        });
        for(Column c : columnArray) {
            if(c.getSort_level()>0) {
                TOrderItem oi = new TOrderItem();
                // 排序columnId
                oi.setKey(c.getId());
                // sort_type  枚举值  1:ASC  2：DESC
                oi.setAscending(c.getSort_type() ==1);
                orders.add(oi);
            }
        }
        return orders;
    }

    public static class AggregateInfo{
        // group by 的列
        private List<Integer> groupKeyList;
        // 聚合操作
        private List<AggregateAction> aaList;

        public AggregateInfo(List<Integer> groupKeyList,
                List<AggregateAction> aaList) {
            this.groupKeyList = groupKeyList;
            this.aaList = aaList;
        }

        public List<Integer> getGroupKeyList() {
            return groupKeyList;
        }

        public void setGroupKeyList(List<Integer> groupKeyList) {
            this.groupKeyList = groupKeyList;
        }

        public List<AggregateAction> getAaList() {
            return aaList;
        }

        public void setAaList(List<AggregateAction> aaList) {
            this.aaList = aaList;
        }
    }

    /**
     * 翻译filter squid,包括流式/批处理
     * @param squid
     */
    public static TFilterExpression translateTFilterExpression(Squid squid,
            Map<String, DSVariable> variableMap,
            Map<String, Squid> name2squidMap) {
        String filterString = squid.getFilter().trim();

        if (!ValidateUtils.isEmpty(filterString)){
            // 将 sql表达式转换为 java表达式。 TODO 表达式解析存在问题
            String fs = filterString.replaceAll("(?<=[^><=!])=(?=[^><=])", " == ")
                    .replaceAll("\\s(?i)and", " && ")
                    .replaceAll("\\s(?i)or", " || ")
                    //					.replaceAll("#|(\\w+)\\.(?!\\d+)", "$1\\$")
                    // 修改正则表达式，解决括号内部存在 a.b
                    .replaceAll("\\s#|((\\s\\w+)|(^\\w+))\\.(?!\\d+)", "$1\\$")
                    .replace("'", "\"")
                    .replace("<>", "!=");

            // 建立变量映射。查找变量
            Matcher matcher = StringUtils.match(fs, "(\\w+\\$\\w+)|(\\$\\w+)");
            Map<String,Integer> keyMap = new HashMap<String, Integer>();
            boolean isExceptionSquid = false;
            if(squid instanceof ExceptionSquid) {
                isExceptionSquid = true;
            }
            while(matcher.find()){
                String var = matcher.group();
                Integer key= lookupVarKey(var, name2squidMap);
                // exception squid 中使用的是负数
                if(isExceptionSquid) {
                    key = -key;
                }
                keyMap.put(var, key);
            }
            // 预处理时间，将时间转换成long 类型。
            Matcher m = StringUtils.match(fs, "'([\\d\\:\\-\\s\\.]+?)'");
            String tmp = fs;
            while (m.find()) {
                String expVal = m.group(1);
                Date date = expressionToDate(expVal);
                if (date != null) {
                    tmp = tmp.replace(m.group(),date.getTime() + "");
                }
            }

            TFilterExpression tfe = new TFilterExpression();
            // 查找表达式中的变量， @name
            List<String> varList = new ArrayList<>();
            tmp = replaceVariableForNameToJep(tmp,varList);
            Map<String, Object> varMap = new HashMap<>();
            for(String str : varList) {
                varMap.put("$"+str, VariableUtil.variableValue(str, variableMap, squid));
            }
            tfe.setVarMap(varMap);
            tfe.setExpression(tmp);
            tfe.setKeyMap(keyMap);
            return tfe;

        }

        return null;
    }
    public static String replaceVariableForNameToJep(String str, List<String> variableNames) {
        HashMap map = new HashMap();
        Pattern pattern = Pattern.compile("\\\'[^\\\']*\\\'");
        Matcher m = pattern.matcher(" " + str + " ");
        StringBuffer sb = new StringBuffer();

        for(boolean result = m.find(); result; result = m.find()) {
            String pattern1 = com.eurlanda.datashire.utility.StringUtils.generateGUID();
            m.appendReplacement(sb, pattern1);
            map.put(pattern1, m.group());
        }

        m.appendTail(sb);
        Pattern pattern11 = Pattern.compile("\\W@([_A-Za-z0-9]*)\\W");
        Matcher m1 = pattern11.matcher(sb.toString());
        StringBuffer sb1 = new StringBuffer();

        String temp;
        for(boolean result1 = m1.find(); result1; result1 = m1.find()) {
            temp = m1.group();
            if(temp.startsWith("\"")&&temp.endsWith("\"")){
                continue;
            }
            if(!temp.contains("@@")) {
                String reqI = temp.replaceAll("\\W", "");
                variableNames.add(reqI);
                temp = temp.replace("@" + reqI, "\\$" + reqI);
                m1.appendReplacement(sb1, temp);
            }
        }

        m1.appendTail(sb1);
        temp = sb1.toString();
        String ss;
        if(map != null && map.keySet().size() > 0) {
            for(Iterator repI1 = map.keySet().iterator(); repI1.hasNext(); temp = temp.replace(ss, map.get(ss) + "")) {
                ss = (String)repI1.next();
            }
        }

        if(!com.eurlanda.datashire.utility.StringUtils.isEmpty(temp)) {
            temp = temp.substring(1);
            temp = temp.substring(0, temp.length() - 1);
        }

        return temp;
    }

    /**
     * 将transformation 中 过滤字符串转换成filter类。 a>1 and b<2 and c=5 or dd=3 ad
     *
     * @param filterString
     *            过滤字符串
     * @return
     */
    public static TFilterExpression translateTransformationFilter(String filterString,
            DataSquid squid, Map<Integer, TTransformation> trsCache,
            Map<String, DSVariable> variableMap) {
        if(ValidateUtils.isEmpty(filterString)){
            return null;
        }
        log.debug("解析Transformation 过滤表达式: " + filterString);
        String fs = filterString.replaceAll("(?<=[^><=!])=(?=[^><=])", " == ")
                .replaceAll("\\s(?i)and", " && ")
                .replaceAll("\\s(?i)or", " || ")
                .replaceAll("#|(\\w+)\\.(?!\\d+)", "$1\\$")
                .replaceAll("#|([\\u4e00-\\u9fa5]+)\\.(?!\\d+)","$1\\$") //匹配  `中文`.columnName
                .replace("'", "\"")
                .replace("<>", "!=");

        // 建立变量映射。查找变量
        Matcher  matcher = StringUtils.match(fs, "(\\w*\\$\\w+)|([\\w\\u4e00-\\u9fa5]*\\$\\w+)");
        Map<String,Integer> keyMap = new HashMap<String, Integer>();
        while(matcher.find()){
            String var = matcher.group();
            Integer key= lookupVarKey(var, squid, trsCache);
            keyMap.put(var, key);
        }
        // 预处理时间，将时间转换成long 类型。
        Matcher m = StringUtils.match(fs, "'([\\d\\:\\-\\s\\.]+?)'");
        String tmp = fs;
        while (m.find()) {
            String expVal = m.group(1);
            Date date = expressionToDate(expVal);
            if (date != null) {
                tmp = tmp.replace(m.group(),date.getTime() + "");
            }
        }
        TFilterExpression tfe = new TFilterExpression();
        // 查找表达式中的变量， @name
        List<String> varList = new ArrayList<>();
        tmp = StringUtil.ReplaceVariableForNameToJep(tmp, varList);
        Map<String, Object> varMap = new HashMap<>();
        for(String str : varList) {
            varMap.put("$"+str, VariableUtil.variableValue(str, variableMap, squid));
        }
        tfe.setVarMap(varMap);
        tfe.setExpression(tmp);

        tfe.setKeyMap(keyMap);

        return tfe;
    }

    private static Date expressionToDate(String expVal) {
        Date date = null;
        if (!ValidateUtils.isNumeric(expVal)) {
            try {
                date = DateTimeUtils.parserDateTime(expVal, "yyyy-MM-dd hh:mm:ss.SSS");
            } catch (Exception e3) {
                try {
                    date = DateTimeUtils.parserDateTime(expVal, "yyyy-MM-dd hh:mm:ss");
                } catch (Exception e2) {
                    try {
                        date = DateTimeUtils.parserDateTime(expVal, "yyyy-MM-dd");
                    } catch (Exception e) {
                        throw new RuntimeException("不支持的日期格式："+expVal);
                    }

                }
            }

        }
        return date;
    }

    /**
     * 取得filter表达式中变量或者常量对应的dataCell.
     * @param val
     * @return
     */
    private static Integer lookupVarKey(String val, DataSquid dataSquid, Map<Integer, TTransformation> trsCache){

        if(val.startsWith("$")){  		// transformation, 从Trs过滤表达式中取依赖的Trs的outKey.
            Transformation tf = getTranByName(val.replace("$", ""), dataSquid);
            TTransformation ttf = trsCache.get(tf.getId());
            List<Integer> outKeys = ttf.getOutKeyList();
            Integer outKey =  outKeys.get(0);
            String index = StringUtils.find(val, "\\[(\\d+)\\]", 1);
            if(!ValidateUtils.isEmpty(index)){
                outKey= outKeys.get(Integer.parseInt(index));
            }
            return outKey;
        }else{		//默认为referenceColumn. 语法为 SquidName.columnName
            if(val.contains("$")){
                String squidName = val.split("\\$")[0];
                String colName= val.split("\\$")[1];

                for(ReferenceColumn col: dataSquid.getSourceColumns()){
                    if(colName.equals(col.getName()) && squidName.equals(col.getGroup_name())){
                        return -col.getColumn_id();
                    }
                }
            }
            throw new RuntimeException("未找到变量"+val+"对应的referenceColumn.");
        }
    }

    /**
     * 按名字找到transformation
     *
     * @param name
     * @return
     */
    public static Transformation getTranByName(String name, DataSquid dataSquid) {
        for (Transformation tf : dataSquid.getTransformations()) {
            if (name.equals(tf.getName())) {
                return tf;
            }
        }
        return null;
    }

    /**
     * 取得filter表达式中变量或者常量对应的key.
     * @param val
     * @return
     */
    private static Integer lookupVarKey(String val, Map<String, Squid> name2squidMap){
        if(val.contains("$")){
            String squidName = val.split("\\$")[0];
            val= val.split("\\$")[1];
            List<Column> columns = null;
            Squid squid = name2squidMap.get(squidName);
            if(squid instanceof DataSquid) {
                columns = ((DataSquid)squid).getColumns();
            } else {
                throw new RuntimeException("不存在该类型的SQUID,可以获取columns -> " + squid);
            }
            //			DataSquid ds = (DataSquid) this.ctx.getSquidByName(squidName);
            for(Column col: columns){
                if(val.equals(col.getName())){

                    return col.getId();
                }
            }
        }
        throw new RuntimeException("未找到变量"+val+"对应的referenceColumn.");
    }

    /**
     * 将infoMap 中可以出现变量的值进行转换
     * @param infoMap
     * @return
     */
    public static Map<String, Object> convertVariable(Map<String, Object> infoMap, Map<String, DSVariable> variableMap, Squid squid) {
        /**
         reg_expression
         term_index
         delimiter
         replica_count
         length
         power
         modulus
         description
         //		 constant_value
         tran_condition
         start_position
         */
        // reg_expression
        String str = (String)infoMap.get(TTransformationInfoType.REG_EXPRESSION.dbValue);
        if(isNotEmpty(str)) {
            // 判断是否为变量,如果是变量，需要将变量的值放入，不是变量则将字符转换为对应值
            if(StringUtils.isVariable(str)) {
                String varName = str.substring(1);
                infoMap.put(TTransformationInfoType.REG_EXPRESSION.dbValue, VariableUtil.variableValue(varName, variableMap, squid));
            } else {
                infoMap.put(TTransformationInfoType.REG_EXPRESSION.dbValue, StringUtils.scriptString(str));
            }
        }

        // term_index
        str = (String)infoMap.get(TTransformationInfoType.TERM_INDEX.dbValue);
        if(isNotEmpty(str)) {
            // 判断是否为变量,如果是变量，需要将变量的值放入，不是变量则将字符转换为对应值
            if(StringUtils.isVariable(str)) {
                String varName = str.substring(1);
                infoMap.put(TTransformationInfoType.TERM_INDEX.dbValue, VariableUtil.variableValue(varName, variableMap, squid));
            } else {
                infoMap.put(TTransformationInfoType.TERM_INDEX.dbValue, Integer.parseInt(str));
            }
        }

        // delimiter
        str = (String)infoMap.get(TTransformationInfoType.DELIMITER.dbValue);
        if(isNotEmpty(str)) {
            // 判断是否为变量,如果是变量，需要将变量的值放入，不是变量则将字符转换为对应值
            if(StringUtils.isVariable(str)) {
                String varName = str.substring(1);
                infoMap.put(TTransformationInfoType.DELIMITER.dbValue, VariableUtil.variableValue(varName, variableMap, squid));
            } else {
                infoMap.put(TTransformationInfoType.DELIMITER.dbValue, StringUtils.scriptString(str));
            }
        }

        // replica_count
        str = (String)infoMap.get(TTransformationInfoType.REPEAT_COUNT.dbValue);
        if(isNotEmpty(str)) {
            // 判断是否为变量,如果是变量，需要将变量的值放入，不是变量则将字符转换为对应值
            if(StringUtils.isVariable(str)) {
                String varName = str.substring(1);
                infoMap.put(TTransformationInfoType.REPEAT_COUNT.dbValue, VariableUtil.variableValue(varName, variableMap, squid));
            } else {
                infoMap.put(TTransformationInfoType.REPEAT_COUNT.dbValue, Integer.parseInt(str));
            }
        }

        // length
        str = (String)infoMap.get(TTransformationInfoType.LENGTH.dbValue);
        if(isNotEmpty(str)) {
            // 判断是否为变量,如果是变量，需要将变量的值放入，不是变量则将字符转换为对应值
            if(StringUtils.isVariable(str)) {
                String varName = str.substring(1);
                infoMap.put(TTransformationInfoType.LENGTH.dbValue, VariableUtil.variableValue(varName, variableMap, squid));
            } else {
                infoMap.put(TTransformationInfoType.LENGTH.dbValue, Integer.parseInt(str));
            }
        }

        // power
        str = (String)infoMap.get(TTransformationInfoType.POWER.dbValue);
        if(isNotEmpty(str)) {
            // 判断是否为变量,如果是变量，需要将变量的值放入，不是变量则将字符转换为对应值
            if(StringUtils.isVariable(str)) {
                String varName = str.substring(1);
                infoMap.put(TTransformationInfoType.POWER.dbValue, VariableUtil.variableValue(varName, variableMap, squid));
            } else {
                infoMap.put(TTransformationInfoType.POWER.dbValue, Double.parseDouble(str));
            }
        }

        // modulus
        str = (String)infoMap.get(TTransformationInfoType.MODULUS.dbValue);
        if(isNotEmpty(str)) {
            // 判断是否为变量,如果是变量，需要将变量的值放入，不是变量则将字符转换为对应值
            if(StringUtils.isVariable(str)) {
                String varName = str.substring(1);
                infoMap.put(TTransformationInfoType.MODULUS.dbValue, VariableUtil.variableValue(varName, variableMap, squid));
            } else {
                infoMap.put(TTransformationInfoType.MODULUS.dbValue, Long.parseLong(str));
            }
        }

        // description
        /**  描述信息，不能使用变量，故不需要加单引号,  2015-6-8 老板提出修改
         str = (String)infoMap.get(TTransformationInfoType.DESCRIPTION.dbValue);
         if(isNotEmpty(str)) {
         // 判断是否为变量,如果是变量，需要将变量的值放入，不是变量则将字符转换为对应值
         if(StringUtils.isVariable(str)) {
         String varName = str.substring(1);
         infoMap.put(TTransformationInfoType.DESCRIPTION.dbValue, VariableUtil.variableValue(varName, ctx, squid));
         } else {
         infoMap.put(TTransformationInfoType.DESCRIPTION.dbValue, StringUtils.scriptString(str));
         }
         }
         */

        // tran_condition
        str = (String)infoMap.get(TTransformationInfoType.TRAN_CONDITION.dbValue);
        if(isNotEmpty(str)) {
            // 判断是否为变量,如果是变量，需要将变量的值放入，不是变量则将字符转换为对应值
            if(StringUtils.isVariable(str)) {
                String varName = str.substring(1);
                infoMap.put(TTransformationInfoType.TRAN_CONDITION.dbValue, VariableUtil.variableValue(varName, variableMap, squid));
            } else {
                infoMap.put(TTransformationInfoType.TRAN_CONDITION.dbValue, StringUtils.scriptString(str));
            }
        }

        // start_position
        str = (String)infoMap.get(TTransformationInfoType.START_POSITION.dbValue);
        if(isNotEmpty(str)) {
            // 判断是否为变量,如果是变量，需要将变量的值放入，不是变量则将字符转换为对应值
            if(StringUtils.isVariable(str)) {
                String varName = str.substring(1);
                infoMap.put(TTransformationInfoType.START_POSITION.dbValue, VariableUtil.variableValue(varName, variableMap, squid));
            } else {
                // 数字类型不需要去除 ''
                infoMap.put(TTransformationInfoType.START_POSITION.dbValue, str);
            }
        }

        return infoMap;
    }

    private static boolean isNotEmpty(String str) {
        return str != null && str.length() > 0;
    }

    /**
     * 获取该squid 对应column 集合
     * @param squid
     * @return
     */
    public static List<Column> getColumnsFromSquid(Squid squid) {
        if(squid == null) {
            throw new TranslateException("该squid 不能为空");
        }
        List<Column> cList = null;
        if(squid instanceof DataSquid) {
            cList = ((DataSquid)squid).getColumns();
        } else if(squid instanceof DocExtractSquid) {
            cList = ((DocExtractSquid)squid).getColumns();
        } else {
            log.error("没有发现对应的SQUID 类型");
            throw new RuntimeException("没有发现对应的SQUID 类型" + squid.getClass().getCanonicalName());
        }
        return cList;
    }

    /**
     * 通过squid 获取该squid对应column 的映射关系
     * @param squid
     * @return tupl3[columnName, dataType, 是否为空]
     */
    public static Map<Integer, TStructField> getId2ColumnsFromSquid(Squid squid) {
        List<Column> cs = TranslateUtil.getColumnsFromSquid(squid);
        Map<Integer, TStructField> i2c = new HashMap<>();

        for(Column c : cs) {
            i2c.put(c.getId(), new TStructField(c.getName(), TDataType.sysType2TDataType(c.getData_type()), c.isNullable(), c.getPrecision(), c.getScale()));
        }
        return i2c;
    }

    /**
     * pivot获取column 获取当前column的集合，但是名字是refColumn的名字
     * @param squid
     * @return
     */
    public static Map<Integer,TStructField> getId2ColumnsFromSquidByNameIsRefName(Squid squid){
        Map<Integer, TStructField> i2c = new HashMap<>();
        List<Column> cs = TranslateUtil.getColumnsFromSquid(squid);
        DataSquid dataSquid = (DataSquid) squid;
        List<ReferenceColumn> refCol = dataSquid.getSourceColumns();
        List<TransformationLink> transLinks = dataSquid.getTransformationLinks();
        List<Transformation> transformations = dataSquid.getToTransformations();
        List<Transformation> fromTranss = dataSquid.getFromTransformations();
        //获取pivot列
        PivotSquid pivotSquid = (PivotSquid) squid;
        ReferenceColumn pivotColumn = getRefColumnById(dataSquid,pivotSquid.getPivotColumnId());
        pivotSquid.getPivotColumnId();
        for(Column column : cs){
            String name = column.getName();
            if(!column.isIs_groupby()){
                //如果不是分组列,就说明是pivot列,那么column的名字取description的名字
                if(column.getDescription()==null){
                    name="null";
                } else if(column.getDescription().length()==0){
                    name = "";
                }else {
                    //如果是TimeStamp类型的，将column名字改为long
                    if(TDataType.sysType2TDataType(pivotColumn.getData_type()) == TDataType.TIMESTAMP){
                        Timestamp tim = (Timestamp) TColumn.toTColumnValue(column.getDescription(),TDataType.TIMESTAMP);
                        name = tim.getTime()*1000+"";
                    } else if(TDataType.sysType2TDataType(pivotColumn.getData_type()) == TDataType.DATE){
                        //如果是Date类型，将column名字转换成long,并只精确到天
                        java.sql.Date date = (java.sql.Date) TColumn.toTColumnValue(column.getDescription(),TDataType.DATE);
                        //精确到天
                        name = (int)Math.ceil(date.getTime()*1.0/1000/60/60/24)+"";
                    } else {
                        name = column.getDescription();
                    }
                }
            } else {
                for (Transformation dt : transformations) {
                    if (dt.getColumn_id() == column.getId()) {
                        int fromTransId = 0;
                        int refColumnId = 0;
                        for (TransformationLink transLink : transLinks) {
                            if (transLink.getTo_transformation_id() == dt.getId()) {
                                fromTransId = transLink.getFrom_transformation_id();
                                break;
                            }
                        }
                        for (Transformation fromTran : fromTranss) {
                            //取出fromColumn对应的columnId
                            if (fromTran.getId() == fromTransId) {
                                refColumnId = fromTran.getColumn_id();
                                break;
                            }
                        }
                        //取出refColumn的名字
                        for (ReferenceColumn ref : refCol) {
                            if (ref.getColumn_id() == refColumnId) {
                                name = ref.getName();
                                break;
                            }
                        }
                        break;
                    }
                }
            }
            //设置
            i2c.put(column.getId(), new TStructField(name, TDataType.sysType2TDataType(column.getData_type()), column.isNullable(), column.getPrecision(), column.getScale()));

        }
        return i2c;
    }
    /**
     * 通过变量名获取变量
     * @param var
     * @param variableName
     * @param squid
     * @return
     */
    public static DSVariable getVariable(Map<String, DSVariable> var, String variableName, Squid squid) {
        if(squid == null) {
            return var.get(variableName);
        } else {
            DSVariable va = null;
            if((va = var.get(variableName)) != null
                    || (va = var.get(VariableUtil.squidVariableKey(variableName, squid.getId()))) != null) {
                return va;
            } else {
                throw new EngineException(squid.getName() + " 该变量[" + variableName + "]不存在");
            }

        }
    }

    public static List<Map<Integer, TStructField>> getId2ColumnsFormStageSquid(List<SquidJoin> squidJoins, BuilderContext ctx) {

        List<Map<Integer, TStructField>> id2Columns = new ArrayList<>();

        for (SquidJoin squidJoin : squidJoins) {

            // 获取当前参与Join 的squid
            Squid squid = ctx.getSquidById(squidJoin.getJoined_squid_id());
            Map<Integer, TStructField> i2c = TranslateUtil.getId2ColumnsFromSquid(squid);

            id2Columns.add(i2c);

        }
        return id2Columns;
    }

    /**
     * 往 StructField[]中添加一个新列
     * 规则:
     *  1.如果存在跟新添加的列相同名字的列,需要修改名字,在名字后面加1
     *  2.如果+1的也存在,那么再+1,直到没有相同的为止
     * 举例:
     *  StructField[]  [tag,tag1,tag2,tag3]
     *  新增列名        tag
     *  那么处理后的结果是,将tag列改名为tag4, 最后为[tag1,tag2,tag3,tag4]
     * @param sfs
     * @param fieldName
     * @return
     */
    public static StructField[] preAddStructFieldToArray(StructField[] sfs, String fieldName) {
        StructField[] rSF = new StructField[sfs.length];

        int idx = 0;
        for(int i=0; i<sfs.length; i++) {
            String sfn = fieldName;
            if(idx > 0) {
                sfn = fieldName + idx;
            }
            boolean isMatch = false;
            for(StructField sf : sfs) {
                if(sfn.equals(sf.name())) {
                    isMatch = true;
                    break;
                }
            }
            idx ++;
            if(!isMatch) {
                // 没有匹配到,中断返回
                break;
            }
        }

        // 判断是否存在匹配
        if(idx > 0) { // 匹配到了
            // 将 fieldName的那列改名为fieldName + idx
            for(int i=0; i<sfs.length; i++) {
                StructField sf = sfs[i];
                if(fieldName.equals(sf.name())) {
                    rSF[i] = new StructField(sf.name() + idx, sf.dataType(), sf.nullable(), sf.metadata());
                } else {
                    rSF[i] = new StructField(sf.name(), sf.dataType(), sf.nullable(), sf.metadata());
                }
            }
        } else {
            for(int i=0; i<sfs.length; i++) {
                StructField sf = sfs[i];
                rSF[i] = new StructField(sf.name(), sf.dataType(), sf.nullable(), sf.metadata());
            }
        }
        return rSF;

    }

    /**
     * 从 StructField[]中找出与fieldName相同的列,并生成该列改名后的名字
     * 规则:
     *  1.如果存在跟fieldName相同名字的列,需要修改名字,在名字后面加1
     *  2.如果+1的也存在,那么再+1,直到没有相同的为止
     * 举例:
     *  StructField[]  [tag,tag1,tag2,tag3]
     *  fieldName        tag
     *  那么处理后的结果是,将tag列改名为tag4, 最后为[tag1,tag2,tag3,tag4], 返回值为tag4
     *  如果没有找到,返回null
     * @param sfs
     * @param fieldName
     * @return
     */
    public static String genRenameFieldName(StructField[] sfs, String fieldName) {
        int idx = 0;
        for(int i=0; i<=sfs.length; i++) {
            String sfn = fieldName;
            if(idx > 0) {
                sfn = fieldName + idx;
            }
            boolean isMatch = false;
            for(StructField sf : sfs) {
                if(sfn.equals(sf.name())) {
                    isMatch = true;
                    break;
                }
            }
            idx ++;
            if(!isMatch) {
                // 没有匹配到,中断返回
                return sfn;
            }
        }
        return null;
    }

    public static ReferenceColumn getRefColumnById(DataSquid squid,int columnId){
        List<ReferenceColumn> referenceColumns = squid.getSourceColumns();
        ReferenceColumn referenceColumn = null;
        if(referenceColumns!=null){
            for(ReferenceColumn ref : referenceColumns){
                if(ref.getColumn_id()==columnId){
                    referenceColumn = ref;
                    break;
                }
            }
        }
        return referenceColumn;
    }
}
