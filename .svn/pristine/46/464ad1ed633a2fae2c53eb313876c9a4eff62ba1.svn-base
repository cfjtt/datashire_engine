package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.engine.dao.SquidFlowDao;
import com.eurlanda.datashire.engine.entity.TDestESSquid;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.engine.util.PathUtil;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.dest.DestESSquid;
import com.eurlanda.datashire.entity.dest.EsColumn;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 异常处理
 * @author Gene
 *
 */
public class DestESSquidBuilder extends AbsTSquidBuilder {

	public DestESSquidBuilder(BuilderContext ctx, Squid currentSquid) {
		super(ctx, currentSquid);
	}

	@Override
	public List<TSquid> doTranslate(Squid squid) {
		DestESSquid dc = (DestESSquid) squid;

		TDestESSquid tDestESSquid = new TDestESSquid();
		tDestESSquid.setId(squid.getId() + "");
		tDestESSquid.setSquidId(squid.getId());
		tDestESSquid.setEs_nodes(PathUtil.getHosts(dc.getHost()));
		tDestESSquid.setEs_port(PathUtil.getPort(dc.getHost()));
		tDestESSquid.setPath(dc.getEsindex() + "/" + dc.getEstype());
		tDestESSquid.setName(dc.getName());
		SquidFlowDao squidFlowDao = ConstantUtil.getSquidFlowDao();

		List<EsColumn> esColumnList = squidFlowDao.getEsColumns(dc.getId());

		Map<Integer, String> id2name = new HashMap<>();
		String is_mapping_id = null;
		for(EsColumn esColumn : esColumnList) {
			if(esColumn.getIs_persist() == 1) {
				id2name.put(esColumn.getColumn_id(), esColumn.getField_name());
				if(esColumn.getIs_mapping_id() == 1) {
					is_mapping_id = esColumn.getField_name();
				}
			}
		}

		// 设置
		tDestESSquid.setId2name(id2name);
		tDestESSquid.setIs_mapping_id(is_mapping_id);
//		tesSquid.setPreviousSquid(get);
		List<Squid> preSquids = ctx.getPrevSquids(squid);
		if(preSquids == null || preSquids.size() != 1) {
			throw new RuntimeException("DEST ES SQUID 前置squid 异常");
		}
		TSquid preTSquid = ctx.getSquidOut(preSquids.get(0));
		tDestESSquid.setPreviousSquid(preTSquid);

		currentBuildedSquids.add(tDestESSquid);
		return this.currentBuildedSquids;
	}
	
}
