package com.eurlanda.datashire.engine.translation;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.engine.dao.SquidFlowDao;
import com.eurlanda.datashire.engine.entity.TCassandraSquid;
import com.eurlanda.datashire.engine.entity.TColumn;
import com.eurlanda.datashire.engine.entity.TDataType;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.engine.entity.clean.UpdateExtractLastValueInfo;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.translation.extract.CassandraExtractManager;
import com.eurlanda.datashire.engine.translation.extract.ExtractManager;
import com.eurlanda.datashire.engine.util.ConfigurationUtil;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.engine.util.JsonUtil;
import com.eurlanda.datashire.engine.util.VariableUtil;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import com.eurlanda.datashire.utility.StringUtil;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * CassandraSquid 翻译的squid链为: TCassandraSquid  -> [TDataFallSquid]
 *
 * Created by zhudebin on 16/2/23.
 */
public class CassandraSquidBuilder extends AbsTSquidBuilder implements TSquidBuilder {

    private static Log log = LogFactory.getLog(CassandraSquidBuilder.class);

    public CassandraSquidBuilder(BuilderContext ctx, Squid currentSquid) {
        super(ctx, currentSquid);
    }

    @Override
    public List<TSquid> doTranslate(Squid squid) {

        CassandraExtractSquid cassandraExtractSquid = (CassandraExtractSquid)squid;
        // 上游cassandra 连接squid
        CassandraConnectionSquid
                cassandraConnectionSquid = (CassandraConnectionSquid)this.ctx.getPrevSquids(squid).get(0);
        if(StringUtils.isEmpty(cassandraConnectionSquid.getPort())){
            cassandraConnectionSquid.setPort(ConfigurationUtil.getProperty("CASSANDRA_PORT"));
        }
        // 翻译 TCassandraSquid
        TCassandraSquid tCassandraSquid = new TCassandraSquid();
        tCassandraSquid.setConnectionSquidId(cassandraConnectionSquid.getId());

        tCassandraSquid.setTableName(getExtractSquidTableName(cassandraExtractSquid.getId(),
                cassandraExtractSquid.getSource_table_id()));
        tCassandraSquid.setAlias(cassandraExtractSquid.getName());

        // 添加增量表达式
        // 判断是否是增量抽取
        if(cassandraExtractSquid.is_incremental()) {
            // 增量方式
            int incrementalMode = cassandraExtractSquid.getIncremental_mode();
            // 检查源列 // 使用的是sourceColumnId
            int sourceColumnId = cassandraExtractSquid.getCheck_column_id();
            SquidFlowDao squidFlowDao = ConstantUtil.getSquidFlowDao();
            SourceColumn sourceColumn = squidFlowDao.getSourceColumnById(sourceColumnId);

            if(sourceColumn == null) {
                throw new EngineException("元数据异常,没有找到对应的sourcecolumn,id:" + sourceColumnId);
            }
            String checkColumn = sourceColumn.getName();

            // 最后的值
            String lastValue = cassandraExtractSquid.getLast_value();
            ExtractManager extractManager = new CassandraExtractManager(
                    cassandraConnectionSquid.getHost().split(","),
                    Integer.parseInt(cassandraConnectionSquid.getPort()),
                    cassandraConnectionSquid.getVerificationMode(),
                    cassandraConnectionSquid.getUsername(),
                    cassandraConnectionSquid.getPassword(),
                    cassandraConnectionSquid.getKeyspace(),
                    cassandraConnectionSquid.getCluster());

            String[] incrementalFilterStringAndMaxValue =  extractManager.genIncrementalFilterStringAndMaxValue(
                    incrementalMode, tCassandraSquid.getTableName(), checkColumn, lastValue,0);
            String filter = incrementalFilterStringAndMaxValue[0];
            String newLastValue = incrementalFilterStringAndMaxValue[1];
            if(StringUtils.isEmpty(cassandraExtractSquid.getFilter())) {
                cassandraExtractSquid.setFilter(filter);
            } else {
                cassandraExtractSquid.setFilter(cassandraExtractSquid.getFilter()
                        + " and " + filter);
            }

            // 判断是否有增量,如果最新的值和上一次运行的值相同,则没有增量数据
            if((newLastValue != null && !newLastValue.equals(lastValue))) {
                // 有最新值
                // 设置需要更新的lastValue值
                tCassandraSquid.setUpdateExtractLastValueInfo(
                        new UpdateExtractLastValueInfo(incrementalFilterStringAndMaxValue[1],
                                cassandraExtractSquid.getId()));
            } else {
                tCassandraSquid.setExistIncrementalData(false);
                log.info("lastValue:" + lastValue + ", newLastValue:" + newLastValue);
            }


        }
        this.setFilter(cassandraExtractSquid,tCassandraSquid);
        tCassandraSquid.setHost(cassandraConnectionSquid.getHost());
        tCassandraSquid.setPort(cassandraConnectionSquid.getPort());
        tCassandraSquid.setCluster(cassandraConnectionSquid.getCluster());
        tCassandraSquid.setKeyspace(cassandraConnectionSquid.getKeyspace());
        tCassandraSquid.setValidate_type(cassandraConnectionSquid.getVerificationMode());
        tCassandraSquid.setUsername(cassandraConnectionSquid.getUsername());
        tCassandraSquid.setPassword(cassandraConnectionSquid.getPassword());

        List<TColumn> columns = new ArrayList<>();
        // 获取 referenceColumn
        for(ReferenceColumn rc : cassandraExtractSquid.getSourceColumns()) {
            // 判断referenceColumn 是否有线连着,有连线的才导出
            Column column = super.getColumnByRefColumnId(cassandraExtractSquid, rc.getColumn_id());
            if(column != null) {
                // 该列需要导出
                TColumn tColumn = new TColumn();
                // 直接设置为columnId, 这里下游暂不添加transformationSquid
                tColumn.setId(column.getId());
                tColumn.setName(column.getName());
                tColumn.setRefName(rc.getName());
                tColumn.setData_type(TDataType.sysType2TDataType(column.getData_type()));
                tColumn.setDbBaseDatatype(DbBaseDatatype.parse(rc.getData_type()));
                tColumn.setPrecision(column.getPrecision());
                tColumn.setScale(column.getScale());
                tColumn.setSourceColumn(true);
                tColumn.setNullable(column.isNullable());
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

        tCassandraSquid.setSquidId(squid.getId());
        tCassandraSquid.setColumns(columns);
        currentBuildedSquids.add(tCassandraSquid);

        /** todo 暂时不添加transformation
        Map<String, Object> customInfo = new HashMap<>();
        customInfo.put(TTransformationInfoType.HBASE_VARBINARY_CONVERT.dbValue, "hbase");
        TSquid ret12 = doExtractTranslateTransformation(cassandraExtractSquid, customInfo);
        if (ret12 != null)
            currentBuildedSquids.add(ret12);
         */

        TSquid dfSquid = doTranslateDataFall(cassandraExtractSquid);
        if(dfSquid != null) {
            currentBuildedSquids.add(dfSquid);
        }
        // 增量/全量
        TSquid ret6 = doTranslateProcessMode(cassandraExtractSquid);
        if (ret6 != null)
            currentBuildedSquids.add(ret6);
        return this.currentBuildedSquids;
    }

    /**
     * 可以抽象出来
     * @param extractSquid
     * @param tds
     */
    public  void setFilter(CassandraExtractSquid extractSquid, TCassandraSquid tds) {
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
