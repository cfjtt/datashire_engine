package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.engine.dao.SquidFlowDao;
import com.eurlanda.datashire.engine.entity.TDestSystemHiveSquid;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.engine.entity.TStructField;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.dest.DestHDFSColumn;
import com.eurlanda.datashire.entity.dest.DestHiveColumn;
import com.eurlanda.datashire.entity.dest.DestHiveSquid;
import org.apache.spark.sql.SaveMode;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * DestHiveSquid
 * @author zdb
 *
 */
public class DestSystemHiveSquidBuilder extends AbsTSquidBuilder {

	public DestSystemHiveSquidBuilder(BuilderContext ctx, Squid currentSquid) {
		super(ctx, currentSquid);
	}

	@Override
	public List<TSquid> doTranslate(Squid squid) {
		DestHiveSquid dc = (DestHiveSquid) squid;

		TDestSystemHiveSquid tDestSystemHiveSquid = new TDestSystemHiveSquid();
		tDestSystemHiveSquid.setId(squid.getId() + "");
		tDestSystemHiveSquid.setSquidId(squid.getId());
        tDestSystemHiveSquid.setSquidName(dc.getName());

        tDestSystemHiveSquid.setTableName(dc.getDb_name() + "." + dc.getTable_name());

        SaveMode saveMode = null;
        switch (dc.getSave_type()) {
        case 0:
            saveMode = SaveMode.Append;
            break;
        case 1:
            saveMode = SaveMode.Overwrite;
            break;
        case 2:
            saveMode = SaveMode.Ignore;
            break;
        case 3:
            saveMode = SaveMode.ErrorIfExists;
            break;
        default:
            throw new EngineException("不存在该保存模式:" + dc.getSave_type());
        }
        tDestSystemHiveSquid.setSaveMode(saveMode);

		tDestSystemHiveSquid.setName(dc.getName());
		SquidFlowDao squidFlowDao = ConstantUtil.getSquidFlowDao();

        // 提取上游squid的column 与id映射关系
        List<Squid> preSquids = this.ctx.getPrevSquids(dc);
        if(preSquids.size() == 0) {
            throw new RuntimeException("未找到本squid的前置squid.");
        }
        Map<Integer, TStructField> preId2column = TranslateUtil.getId2ColumnsFromSquid(preSquids.get(0));

        tDestSystemHiveSquid.setPreId2Columns(preId2column);
        List<DestHiveColumn> destHiveColumns = squidFlowDao.getHiveColumns(dc.getId());
        List<Tuple2<String, TStructField>> hiveColumns = new ArrayList<>();

        Collections.sort(destHiveColumns, new Comparator<DestHDFSColumn>() {
            @Override public int compare(DestHDFSColumn o1, DestHDFSColumn o2) {
                return o1.getColumn_order() - o2.getColumn_order();
            }
        });
        for(DestHiveColumn c : destHiveColumns) {
            TStructField tsf = preId2column.get(c.getColumn_id());
            hiveColumns.add(new Tuple2<String, TStructField>(c.getField_name(), new TStructField(tsf.getName(), tsf.getDataType(), true, c.getPrecision(), c.getScale())));
        }
        tDestSystemHiveSquid.setHiveColumns(hiveColumns);

		TSquid preTSquid = ctx.getSquidOut(preSquids.get(0));
		tDestSystemHiveSquid.setPreTSquid(preTSquid);

		currentBuildedSquids.add(tDestSystemHiveSquid);
		return this.currentBuildedSquids;
	}
	
}
