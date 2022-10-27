package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.engine.entity.TColumn;
import com.eurlanda.datashire.engine.entity.TDataType;
import com.eurlanda.datashire.engine.entity.TMongoExtractSquid;
import com.eurlanda.datashire.engine.entity.TNoSQLDataSource;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.engine.entity.TTransformation;
import com.eurlanda.datashire.engine.entity.TTransformationAction;
import com.eurlanda.datashire.engine.entity.TTransformationSquid;
import com.eurlanda.datashire.engine.enumeration.TTransformationInfoType;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.engine.util.DateUtil;
import com.eurlanda.datashire.engine.util.cool.JList;
import com.eurlanda.datashire.engine.util.cool.JMap;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.MongodbExtractSquid;
import com.eurlanda.datashire.entity.NOSQLConnectionSquid;
import com.eurlanda.datashire.entity.ReferenceColumn;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.entity.TransformationLink;
import com.eurlanda.datashire.enumeration.NoSQLDataBaseType;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 数据库Extract翻译器。
 * 
 * @author Gene
 * 
 */
public class MongodbExctractBuilder extends AbsTSquidBuilder implements TSquidBuilder {

	private boolean removeNoUseKeys = false;

	public MongodbExctractBuilder(BuilderContext ctx, Squid currentSquid) {
		super(ctx, currentSquid);
	}

	@Override
	public List<TSquid> doTranslate(Squid cur) {
		MongodbExtractSquid ms = (MongodbExtractSquid) cur;
		TMongoExtractSquid tms = new TMongoExtractSquid();
		tms.setSquidId(ms.getId());
		// 因为extractSquid前面必定有一个datasourceSquid,直接取第一个
		NOSQLConnectionSquid preSquids = (NOSQLConnectionSquid) this.getPrevSquids().get(0);
		tms.setId(ms.getId() + "");
		tms.setConnectionSquidId(preSquids.getId());
		// 数据来源信息
		TNoSQLDataSource dataSource = new TNoSQLDataSource();
		dataSource.setDbName(preSquids.getDb_name());
		dataSource.setHost(preSquids.getHost());
		dataSource.setPassword(preSquids.getPassword());
		dataSource.setPort(preSquids.getPort());
		dataSource.setType(NoSQLDataBaseType.parse(preSquids.getDb_type()));
		dataSource.setUserName(preSquids.getUser_name());

		dataSource.setTableName(getSourceTableName(ms.getSource_table_id(), ms.getName()));
		// 直接使用过滤表达式，增量、变量功能暂时没有
//		this.setFilter(extractSquid, dataSource);
		dataSource.setFilter(ms.getFilter());
		tms.setDataSource(dataSource);

        List<ReferenceColumn> columns = ms.getSourceColumns();
        Set<TColumn> orderList = new HashSet<>();
        java.util.Collections.sort(columns, new Comparator<ReferenceColumn>() {
            @Override
            public int compare(ReferenceColumn o1, ReferenceColumn o2) {
                return o1.getRelative_order() > o2.getRelative_order() ? 1 : -1;
            }
        });
        int index=0;
        for (ReferenceColumn c : columns) {

            TColumn tColumn = new TColumn(String.valueOf(index++), -c.getColumn_id(), TDataType.STRING);
            Column col = this.getColumnByRefColumnId(ms, c.getColumn_id());
            tColumn.setName(c.getName());
            if(col!=null){
                tColumn.setData_type(TDataType.STRING);
            }
            orderList.add(tColumn);
        }

		tms.setColumnSet(buildDataSquidTColumns(ms));
		tms.setColumnSet(orderList);
		currentBuildedSquids.add(tms);

        // 增加transformation层,执行类型转换,过滤功能
        TSquid tTranSquid = doTranslateTransformation(ms);
        currentBuildedSquids.add(tTranSquid);

		TSquid df = super.doTranslateDataFall(ms);
		if (df != null) {
			currentBuildedSquids.add(df);
		}

        // 增量/全量
        TSquid ret6 = doTranslateProcessMode(ms);
        if (ret6 != null)
            currentBuildedSquids.add(ret6);
		return currentBuildedSquids;
	}

	public String getSourceTableName(int sourceTableId, String squidName){
		if(sourceTableId==0){
			throw new RuntimeException("未找到extractSquid["+squidName+"]引用的sourceTable["+sourceTableId+"]");
		}
		String sql ="select table_name from ds_source_table where id="+sourceTableId;
		Map<String ,Object> ret = ConstantUtil.getJdbcTemplate().queryForMap(sql);

		return (String) ret.get("table_name");
	}

	/**
	 * 加一层transformation,只为了最后一层transformation做数据校验
	 * @param dc
	 * @return
	 */
	private TSquid doTranslateTransformation(MongodbExtractSquid dc) {
		TTransformationSquid ts = new TTransformationSquid();
		List<TTransformationAction> actions = new ArrayList<TTransformationAction>();
		ts.settTransformationActions(actions);

        // 添加 extraction_date
        Column dateCol = getExtractionDateColumn();
        boolean isSysExtractionDate = true;

		for (TransformationLink link : dc.getTransformationLinks()) {
			Transformation from_trs = this.getTrsById(link.getFrom_transformation_id());
			Transformation to_trs = this.getTrsById(link.getTo_transformation_id());
			TTransformationAction action = new TTransformationAction();
			TTransformation tTransformation = new TTransformation();
			tTransformation.setType(TransformationTypeEnum.VIRTUAL);
			tTransformation.setSequenceIndex(1);
			tTransformation.setInKeyList(new JList<>(-from_trs.getColumn_id()));
			tTransformation.setOutKeyList(new JList<>(to_trs.getColumn_id()));

            // 判断是否为系统列 extraction_date
            if(dateCol != null && to_trs.getColumn_id() == dateCol.getId()) {
                isSysExtractionDate = false;
            }

			HashSet<Integer> dtk = getDropedTrsKeys(to_trs);
			dtk.add(-from_trs.getColumn_id());
			action.setRmKeys(dtk);

			Column col = this.getColumnById(to_trs.getColumn_id());
			JMap infoMap = new JMap<>();
			infoMap.put(TTransformationInfoType.STRING_MAX_LENGTH.dbValue, col.getLength());
			infoMap.put(TTransformationInfoType.NUMERIC_PRECISION.dbValue, col.getPrecision());
			infoMap.put(TTransformationInfoType.NUMERIC_SCALE.dbValue, col.getScale());
            infoMap.put(TTransformationInfoType.DOC_VARCHAR_CONVERT.dbValue, "mongodb");
            infoMap.put(TTransformationInfoType.OUTPUT_DATA_TYPE.dbValue, col.getData_type());
			tTransformation.setInfoMap(infoMap);

			action.settTransformation(tTransformation);
			actions.add(action);
		}

		if (dateCol != null && isSysExtractionDate) {
			TTransformationAction action = new TTransformationAction();
			TTransformation tTransformation = new TTransformation();
			tTransformation.setType(TransformationTypeEnum.CONSTANT);
			tTransformation.setSequenceIndex(1);
			tTransformation.setOutKeyList(new JList<Integer>(dateCol.getId()));
			action.settTransformation(tTransformation);
			JMap infoMap = new JMap<>();
			infoMap.put(TTransformationInfoType.CONSTANT_VALUE.dbValue, DateUtil.nowTimestamp());
			infoMap.put(TTransformationInfoType.OUTPUT_DATA_TYPE.dbValue, TDataType.sysType2TDataType(SystemDatatype.DATETIME.value()));
			tTransformation.setInfoMap(infoMap);
			actions.add(action);
		}
		ts.setPreviousSquid(this.getCurLastTSqud());
		return ts;
	}

	/**
	 * 取本squid里面的column
	 * @param colId colid.
	 * @return
	 */
	protected Column getColumnById(int colId){
        MongodbExtractSquid ss = (MongodbExtractSquid) this.currentSquid;
		for (Column col : ss.getColumns()) {
			if (col.getId() == colId) {
				return col;
			}
		}
		return null;
	}
    /**
	private Column getExtractionDateColumn() {
        MongodbExtractSquid de = (MongodbExtractSquid) this.currentSquid;
		for (Column col : de.getColumns()) {
			if (col.getName().equals(Constants.DEFAULT_EXTRACT_COLUMN_NAME)) {
				return col;
			}
		}
		return null;
	}
     */

	/**
	 * 抛弃transformation.并且返回可用的抛弃值。
	 *
	 * @param tf
	 * @return
	 */
	protected HashSet<Integer> getDropedTrsKeys(Transformation tf) {
		HashSet<Integer> ret = new HashSet<>();
		if (!this.removeNoUseKeys) {
            MongodbExtractSquid ss = (MongodbExtractSquid) this.currentSquid;
			for (ReferenceColumn col : ss.getSourceColumns()) {
				boolean contains = false;
				for (Transformation t : this.getBeginTrs()) {
					if (t.getColumn_id() == col.getColumn_id()) {
						contains = true;
					}
				}
				if (!contains) {
					ret.add(col.getColumn_id());
				}
			}
			this.removeNoUseKeys = true;

		}
		return ret;
	}
}
