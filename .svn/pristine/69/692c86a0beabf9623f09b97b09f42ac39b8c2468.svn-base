package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.common.util.HbaseUtil;
import com.eurlanda.datashire.engine.entity.TColumn;
import com.eurlanda.datashire.engine.entity.TDataFallSquid;
import com.eurlanda.datashire.engine.entity.TDataSource;
import com.eurlanda.datashire.engine.entity.TDataType;
import com.eurlanda.datashire.engine.entity.TReportSquid;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.engine.entity.TSquidType;
import com.eurlanda.datashire.engine.util.ConfigurationUtil;
import com.eurlanda.datashire.engine.util.DBUtils;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.DataSquid;
import com.eurlanda.datashire.entity.GISMapSquid;
import com.eurlanda.datashire.entity.ReportSquid;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.enumeration.DataBaseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
/**
 * report builder
 * 包括 报表，地图
 * @author Gene
 * 
 */
public class ReportBuilder extends AbsTSquidBuilder implements TSquidBuilder {
	private Logger logger = LoggerFactory.getLogger(ReportBuilder.class);

	public ReportBuilder(BuilderContext ctx, Squid currentSquid) {
		super(ctx, currentSquid);
	}

	@Override
	public List<TSquid> doTranslate(Squid cur) {
		boolean isRealTime = false;
		if(cur instanceof ReportSquid) {
			isRealTime = ((ReportSquid) cur).isIs_real_time();
		} else if(cur instanceof GISMapSquid) {
			isRealTime = ((GISMapSquid) cur).isIs_real_time();
		}
		TReportSquid tReportSquid = new TReportSquid();
		tReportSquid.setTaskId("");
		tReportSquid.setSquidId(cur.getId());
		tReportSquid.setRepositoryId(ctx.repositoryId);
		List<TSquid> prevList = new ArrayList<TSquid>(); // 翻译后的 reportsquid
															// 关联的squid
		List<Squid> preSquids = this.getPrevSquids(); // 关联到reportSquid的squid.

		if (isRealTime) { // 实时

			for (Squid sq : preSquids) { // 设置prevList
				if (sq instanceof DataSquid) {
					DataSquid ds = (DataSquid) sq;
                    // 如果非落地，那么先添加落地squid.
                    if (!ds.isIs_persisted()) {
                        transPreNotPersistSquid(sq, ds.getColumns(), prevList);
                    } else {
                        TSquid ts = ctx.getSquidDataFallOut(sq);
                        prevList.add(ts);		// 无论如何，前一级squid都是dataFallSquid
                    }
				}
			}

		} else { // 非实时

			for (Squid sq : preSquids) {
				TSquid ts = this.ctx.getSquidDataFallOut(sq);
				if (prevList != null) {
					prevList.add(ts);
				}
			}
		}
		tReportSquid.setPreSquidList(prevList);
		currentBuildedSquids.add(tReportSquid);
		return currentBuildedSquids;
	}

	/**
	 * 翻译实时报表squid前面没有设置落地的squid
	 * 需要添加一个落地squid
	 * @param squid
	 * @param columns
	 * @param prevList
	 */
	private void transPreNotPersistSquid(Squid squid, List<Column> columns, List<TSquid> prevList) {
		TSquid outSquid = this.ctx.getSquidOut(squid);
		TDataFallSquid tdf = new TDataFallSquid();

		TDataSource tDataSource = new TDataSource();
		tDataSource.setType(DataBaseType.HBASE_PHOENIX);
		tDataSource.setTableName(HbaseUtil.genReportTableName(ctx.taskId, squid.getId()));
		tDataSource.setHost(ConfigurationUtil.getInnerHbaseHost() + ":" + ConfigurationUtil.getInnerHbasePort());
		//						tDataSource.setPort(ConfigurationUtil.getInnerHbasePort());

		tdf.setPreviousSquid(outSquid);
		tdf.setDataSource(tDataSource);
		tdf.setSquidId(squid.getId());
		tdf.setType(TSquidType.DATA_FALL_SQUID);

		Set<TColumn> tColumnSet = new HashSet<>();
		for (Column c : columns) {
			//Integer id = ctx.getColumnIdByColumn(c);
			TColumn tColumn = new TColumn();
			tColumn.setId(c.getId());
			tColumn.setName(c.getName());
			tColumn.setLength(c.getLength());
			tColumn.setNullable(c.isNullable());
			tColumn.setPrecision(c.getPrecision());
			tColumn.setPrimaryKey(c.isIsPK());
			tColumn.setData_type(TDataType.sysType2TDataType(c.getData_type()));
			//							if (c.getName().equalsIgnoreCase("ID"))
			//								tColumn.setPrimaryKey(true);
			tColumnSet.add(tColumn);
		}

		tdf.setColumnSet(tColumnSet);

		currentBuildedSquids.add(tdf);
		prevList.add(tdf);

		// 创建落地表
		logger.debug("开始创建Hbase数据库表:"+tDataSource.getTableName());
		DBUtils.createHbaseTable(tDataSource.getTableName(), columns);

	}
}
