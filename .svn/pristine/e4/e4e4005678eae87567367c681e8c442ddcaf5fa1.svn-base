package com.eurlanda.datashire.engine.translation.extract;

import com.eurlanda.datashire.enumeration.DataBaseType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by zhudebin on 2017/5/10.
 */
public abstract class ExtractManager {

    protected Logger logger = LoggerFactory.getLogger(ExtractManager.class);

    /**
     * 中间数据,小心使用,用来缓存中间数据,提高性能
     * 危险
     */
    private Map<String, Object> internalData = new HashMap<>();
    protected final static String FILTER_INTERNAL_DATA_PREFIX = "FILTER_INTERNAL_DATA_PREFIX_";
    protected final static String TABLE_NAME = "_TABLE_NAME_";
    protected final static String COLUMN_NAME = "_COLUMN_NAME_";

    public ExtractManager() {
    }

    protected abstract void init();

    /**
     * 生成增量抽取filter字符串
     * @param incrementalMode 增量配置: incremental_mode, check_column, last_value
     * @return filter,newLastValue
     */
    public String[] genIncrementalFilterStringAndMaxValue(int incrementalMode,
            String tableName, String columnName, String lastValue,Integer limit) {
        setFilterInternalData(TABLE_NAME, tableName);
        setFilterInternalData(COLUMN_NAME, columnName);
        // 判断增量方式
        String maxValue = null;
        if(incrementalMode == 1) {
            maxValue = getMaxValueByColumn(tableName, columnName);
        } else if(incrementalMode == 2) {
            maxValue = getCurrentDsTimestamp();
        } else {
            throw new RuntimeException("不能处理该增量方式:" + incrementalMode);
        }
        String maxValueExpStr = convertValueStrToExpString(maxValue);
        //针对特殊类型进行处理(oracle中NCLOB等特殊类型需要to_char转换)
        StringBuilder filterBuilder = new StringBuilder();
        filterBuilder.append(escapeColName(columnName))
                .append("<=").append(maxValueExpStr);

        if(StringUtils.isNotEmpty(lastValue)) {
            String minValueExpStr = convertValueStrToExpString(lastValue);
            filterBuilder.append(" and ")
                    .append(escapeColName(columnName))
                    .append(">").append(minValueExpStr);
        }
        /*//只有当limit数量大于0时，才限制
        if(limit!=null && limit>0){
            filterBuilder.append("  limit 0,"+limit);
        }*/
        cleanFilterExpInternalData();
        return new String[]{filterBuilder.toString(), maxValue};
    }

    /**
     * 增量抽取，生成filter字段，适用于合表和分流
     * @param incrementalMode
     * @param tableName
     * @param columnName
     * @param lastValue
     * @param limit
     * @return
     */
    public String[] genIncrementalFilterStringAndMaxValue(int incrementalMode,
                                                          Set<String> tableName, String columnName, String lastValue, Integer limit)  {
        String[] tableArray = new String[0];
        tableArray=tableName.toArray(tableArray);
        setFilterInternalData(TABLE_NAME, tableArray[0]);
        setFilterInternalData(COLUMN_NAME, columnName);
        // 判断增量方式
        String maxValue = null;
        boolean isValid = isValidType(incrementalMode);
        if(!isValid){
            throw new RuntimeException("不支持该检查列");
        }
        if(incrementalMode == 1) {
            maxValue = getMaxValueByColumn(tableArray, columnName,lastValue,limit);
        } else if(incrementalMode == 2) {
            maxValue = getCurrentDsTimestamp();
        } else {
            throw new RuntimeException("不能处理该增量方式:" + incrementalMode);
        }
        String maxValueExpStr = convertValueStrToExpString(maxValue);
        //针对特殊类型进行处理(oracle中NCLOB等特殊类型需要to_char转换)
        StringBuilder filterBuilder = new StringBuilder();
        if(maxValue!=null){
            filterBuilder.append(escapeColName(columnName))
                    .append("<=").append(maxValueExpStr);
        }
        if(StringUtils.isNotEmpty(lastValue)) {
            String minValueExpStr = convertValueStrToExpString(lastValue);
            if(maxValue!=null){
                filterBuilder.append(" and ");
            }
            filterBuilder.append(escapeColName(columnName))
                    .append(">").append(minValueExpStr);
        } else {
            //如果最后值不存在，那么增加is null条件，防止存在null值
            if(maxValue!=null){
                filterBuilder.append(" or ");
            }
            filterBuilder.append(escapeColName(columnName)).append(" is null ");
        }
        cleanFilterExpInternalData();
        return new String[]{filterBuilder.toString(), maxValue};
    }
    protected void cleanFilterExpInternalData() {
        List<String> toRemoveKey = new ArrayList<>();
        for(String key : internalData.keySet()) {
            if(key.startsWith(FILTER_INTERNAL_DATA_PREFIX)) {
                toRemoveKey.add(key);
            }
        }
        for(String key : toRemoveKey) {
            internalData.remove(key) ;
        }
    }

    protected abstract String getMaxValueByColumn(String tableName, String columnName);
    protected abstract String getMaxValueByColumn(String[] tableNames, String columnName,String lastValue,int limit);
    protected abstract String getCurrentDsTimestamp();
    protected abstract boolean isValidType(int incrementalMode);
    protected abstract String convertValueStrToExpString(String valueStr);
    protected abstract DataBaseType getDbType();
    /**
     * When using a column name in a generated SQL query, how (if at all)
     * should we escape that column name? e.g., a column named "table"
     * may need to be quoted with backtiks: "`table`".
     *
     * @param colName the column name as provided by the user, etc.
     * @return how the column name should be rendered in the sql text.
     */
    protected String escapeColName(String colName) {
        return colName;
    }

    /**
     * When using a table name in a generated SQL query, how (if at all)
     * should we escape that column name? e.g., a table named "table"
     * may need to be quoted with backtiks: "`table`".
     *
     * @param tableName the table name as provided by the user, etc.
     * @return how the table name should be rendered in the sql text.
     */
    protected String escapeTableName(String tableName) {
        return tableName;
    }

    protected void setFilterInternalData(String key, Object value) {
        internalData.put(FILTER_INTERNAL_DATA_PREFIX + key, value);
    }

    protected Object getFilterInternalData(String key) {
        return internalData.get(FILTER_INTERNAL_DATA_PREFIX + key);
    }

    protected void setInternalData(String key, Object value) {
        internalData.put(key, value);
    }

    protected Object getInternalData(String key) {
        return internalData.get(key);
    }

    public abstract void close();
}
