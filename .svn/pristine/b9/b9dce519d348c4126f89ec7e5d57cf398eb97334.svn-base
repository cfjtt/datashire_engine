package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.engine.dao.SquidFlowDao;
import com.eurlanda.datashire.engine.entity.*;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.dest.DestImpalaColumn;
import com.eurlanda.datashire.entity.dest.DestImpalaSquid;
import com.eurlanda.datashire.enumeration.DataBaseType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * impala 落地squid 翻译
 * @author zdb
 *
 */
public class DestImpalaSquidBuilder extends AbsTSquidBuilder {

	public DestImpalaSquidBuilder(BuilderContext ctx, Squid currentSquid) {
		super(ctx, currentSquid);
	}

	@Override
	public List<TSquid> doTranslate(Squid squid) {
        DestImpalaSquid destSquid = (DestImpalaSquid) squid;

        TDestImpalaSquid tDestSquid = new TDestImpalaSquid();


        SquidFlowDao squidFlowDao = ConstantUtil.getSquidFlowDao();

        List<DestImpalaColumn> columnList = squidFlowDao.getImpalaColumns(destSquid.getId());

        TDataSource tDataSource = new TDataSource();
        tDataSource.setHost(destSquid.getHost());
        tDataSource.setDbName(destSquid.getStore_name());
        tDataSource.setTableName(destSquid.getImpala_table_name());
        tDataSource.setType(DataBaseType.IMPALA);

        Set<TColumn> columns = new HashSet<>();
        for(DestImpalaColumn ic : columnList) {
            if(ic.getIs_dest_column() == 1) {
                TColumn tc = new TColumn();
                tc.setName(ic.getField_name());
                tc.setData_type(TDataType.sysType2TDataType(ic.getColumn().getData_type()));
                tc.setId(ic.getColumn_id());
                columns.add(tc);
            }
        }

        tDestSquid.setDataSource(tDataSource);
        tDestSquid.setColumns(columns);
        tDestSquid.setType(TSquidType.DEST_IMPALA_SQUID);
        // 设置

        List<Squid> preSquids = ctx.getPrevSquids(squid);
        if(preSquids == null || preSquids.size() != 1) {
            throw new RuntimeException("DEST ES SQUID 前置squid 异常");
        }
        TSquid preTSquid = ctx.getSquidOut(preSquids.get(0));
        tDestSquid.setPreviousSquid(preTSquid);
        tDestSquid.setSquidId(destSquid.getId());

        currentBuildedSquids.add(tDestSquid);
        return this.currentBuildedSquids;
	}

}
