package com.eurlanda.datashire.engine.translation;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.engine.entity.*;
import com.eurlanda.datashire.engine.util.JsonUtil;
import com.eurlanda.datashire.engine.util.VariableUtil;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import com.eurlanda.datashire.utility.StringUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * HiveSquid 翻译的squid链为: THiveSquid  -> [TDataFallSquid]
 *
 * Created by zhudebin on 16/2/23.
 */
public class HiveSquidBuilder extends AbsTSquidBuilder implements TSquidBuilder {

    private static Log log = LogFactory.getLog(HiveSquidBuilder.class);

    public HiveSquidBuilder(BuilderContext ctx, Squid currentSquid) {
        super(ctx, currentSquid);
    }

    @Override
    public List<TSquid> doTranslate(Squid squid) {
        SystemHiveExtractSquid hiveExtractSquid = (SystemHiveExtractSquid)squid;
        // 获取上游hive 连接squid
        SystemHiveConnectionSquid
                hiveConnectionSquid = (SystemHiveConnectionSquid)this.ctx.getPrevSquids(squid).get(0);

        // 翻译 THiveSquid
        THiveSquid tHiveSquid = new THiveSquid();
        tHiveSquid.setConnectionSquidId(hiveConnectionSquid.getId());

        tHiveSquid.setTableName(hiveConnectionSquid.getDb_name() + "."
                + getExtractSquidTableName(hiveExtractSquid.getId(), hiveExtractSquid.getSource_table_id()));
        tHiveSquid.setAlias(hiveExtractSquid.getName());
        this.setFilter(hiveExtractSquid,tHiveSquid);
        //tHiveSquid.setFilter(hiveExtractSquid.getFilter());
        List<TColumn> columns = new ArrayList<>();
        // 获取 referenceColumn
        for(ReferenceColumn rc : hiveExtractSquid.getSourceColumns()) {
            // 判断referenceColumn 是否有线连着,有连线的才导出
            Column column = super.getColumnByRefColumnId(hiveExtractSquid, rc.getColumn_id());
            if(column != null) {
                // 该列需要导出
                TColumn tColumn = new TColumn();
                // 直接设置为columnId, 这里下游暂不添加transformationSquid
                tColumn.setId(column.getId());
                tColumn.setName(column.getName());
                tColumn.setRefName(rc.getName());
                tColumn.setData_type(TDataType.sysType2TDataType(column.getData_type()));
                tColumn.setSourceColumn(true);
                tColumn.setDbBaseDatatype(DbBaseDatatype.parse(rc.getData_type()));
                columns.add(tColumn);
            } else {
                // 该列不需要导出
                log.debug("该RC列不需要导出:" + rc.getName());
            }
        }
        // 是否存在系统列-抽取时间列
        Column extractDateColumn = getExtractionDateColumn();
        if(extractDateColumn != null) {
            TColumn tColumn = new TColumn();
            tColumn.setId(extractDateColumn.getId());
            tColumn.setName(extractDateColumn.getName());
            tColumn.setData_type(TDataType.sysType2TDataType(extractDateColumn.getData_type()));
            tColumn.setSourceColumn(false);
            columns.add(tColumn);
        }
        tHiveSquid.setSquidId(squid.getId());
        tHiveSquid.setColumns(columns);
        currentBuildedSquids.add(tHiveSquid);

        /** todo 暂时不添加transformation
        Map<String, Object> customInfo = new HashMap<>();
        customInfo.put(TTransformationInfoType.HBASE_VARBINARY_CONVERT.dbValue, "hbase");
        TSquid ret12 = doExtractTranslateTransformation(hiveExtractSquid, customInfo);
        if (ret12 != null)
            currentBuildedSquids.add(ret12);
         */

        TSquid dfSquid = doTranslateDataFall(hiveExtractSquid);
        if(dfSquid != null) {
            currentBuildedSquids.add(dfSquid);
        }
        // 增量/全量
        TSquid ret6 = doTranslateProcessMode(hiveExtractSquid);
        if (ret6 != null)
            currentBuildedSquids.add(ret6);
        return this.currentBuildedSquids;
    }

    /**
     * 设置extract的过滤
     * 可以抽象出来封装
     * @param extractSquid
     * @param tds
     */
    public  void setFilter(SystemHiveExtractSquid extractSquid, THiveSquid tds) {
        /**
         * 转换过滤条件
         */
        String filterSQL = "";
        if (!ValidateUtils.isEmpty(extractSquid.getFilter())) {
            String squidName = extractSquid.getName();
            filterSQL += extractSquid.getFilter().replace(squidName + ".", "");
        }

        // 替换其中的变量
        List<String> vaList = new ArrayList<>();
        filterSQL = StringUtil.ReplaceVariableForName(filterSQL,vaList);
//		Map<String, DSVariable> variableMap = ctx.getVariable();
        for(String str : vaList) {
            // 重构变量的提取
            Object value = VariableUtil.variableValue(str, ctx.getVariable(), extractSquid);
            String filterValue = "";
            if(value instanceof String){
                filterValue = "'" + value + "'";
            } else {
                filterValue = JsonUtil.toJSONString(value);
            }
            filterSQL = filterSQL.replaceFirst("\\?",filterValue);
        }
        tds.setFilter(org.apache.commons.lang.StringUtils.isEmpty(filterSQL)?null : filterSQL);
    }
}
