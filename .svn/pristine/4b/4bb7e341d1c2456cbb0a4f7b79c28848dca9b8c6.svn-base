package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.engine.dao.SquidFlowDao;
import com.eurlanda.datashire.engine.entity.TDestCassandraSquid;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.engine.entity.TStructField;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.util.ConfigurationUtil;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.entity.CassandraConnectionSquid;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.dest.DestCassandraColumn;
import com.eurlanda.datashire.entity.dest.DestCassandraSquid;
import com.eurlanda.datashire.entity.dest.DestHDFSColumn;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.spark.sql.SaveMode;
import scala.Tuple2;

import java.util.*;

/**
 * DestCassandraSquid
 * @author zdb
 *
 */
public class DestCassandraSquidBuilder extends AbsTSquidBuilder {

	public DestCassandraSquidBuilder(BuilderContext ctx, Squid currentSquid) {
		super(ctx, currentSquid);
	}

	@Override
	public List<TSquid> doTranslate(Squid squid) {
        DestCassandraSquid dc = (DestCassandraSquid) squid;

		TDestCassandraSquid tDestCassandraSquid = new TDestCassandraSquid();
		tDestCassandraSquid.setId(squid.getId() + "");
		tDestCassandraSquid.setSquidId(squid.getId());
        tDestCassandraSquid.setSquidName(dc.getName());

        // 获取cassandra connection信息
        int cassandraConnectionSquidId = dc.getDest_squid_id();
        CassandraConnectionSquid cassandraConnectionSquid = (CassandraConnectionSquid)ctx.getSquidById(cassandraConnectionSquidId);
        if(StringUtils.isEmpty(cassandraConnectionSquid.getPort())){
            cassandraConnectionSquid.setPort(ConfigurationUtil.getProperty("CASSANDRA_PORT"));
        }
        HashMap<String, String> cassandraConnectionInfo = new HashMap<>();
        /** "keyspace","cluster",
        *   "cassandra.validate_type",
        *   "engine.spark.cassandra.username",
        *   "engine.spark.cassandra.password",
        *   "engine.spark.cassandra.host",
        *   "engine.spark.cassandra.port"
        **/
        cassandraConnectionInfo.put("keyspace", cassandraConnectionSquid.getKeyspace());
        cassandraConnectionInfo.put("cluster", cassandraConnectionSquid.getCluster());
        cassandraConnectionInfo.put("cassandra.validate_type", cassandraConnectionSquid.getVerificationMode() + "");
        cassandraConnectionInfo.put("engine.spark.cassandra.username", cassandraConnectionSquid.getUsername());
        cassandraConnectionInfo.put("engine.spark.cassandra.password", cassandraConnectionSquid.getPassword());
        cassandraConnectionInfo.put("engine.spark.cassandra.host", cassandraConnectionSquid.getHost());
        cassandraConnectionInfo.put("engine.spark.cassandra.port", cassandraConnectionSquid.getPort());
        tDestCassandraSquid.setCassandraConnectionInfo(cassandraConnectionInfo);
        tDestCassandraSquid.setTableName(dc.getTable_name());

        SaveMode saveMode = null;
        switch (dc.getSave_type()) {
        case 0:
            saveMode = SaveMode.Append;
            break;
        case 1:
            saveMode = SaveMode.Overwrite;
            break;
        default:
            throw new EngineException("不存在该保存模式:" + dc.getSave_type());
        }
        tDestCassandraSquid.setSaveMode(saveMode);

		tDestCassandraSquid.setName(dc.getName());
		SquidFlowDao squidFlowDao = ConstantUtil.getSquidFlowDao();

        // 提取上游squid的column 与id映射关系
        List<Squid> preSquids = this.ctx.getPrevSquids(dc);
        if(preSquids.size() == 0) {
            throw new RuntimeException("未找到本squid的前置squid.");
        }
        Map<Integer, TStructField> preId2column = TranslateUtil.getId2ColumnsFromSquid(preSquids.get(0));

        tDestCassandraSquid.setPreId2Columns(preId2column);
        List<DestCassandraColumn> destCassandraColumns = squidFlowDao.getCassandraDestColumns(dc.getId());
        List<Tuple2<String, TStructField>> destColumns = new ArrayList<>();

        Collections.sort(destCassandraColumns, new Comparator<DestHDFSColumn>() {
            @Override public int compare(DestHDFSColumn o1, DestHDFSColumn o2) {
                return o1.getColumn_order() - o2.getColumn_order();
            }
        });
        for(DestCassandraColumn c : destCassandraColumns) {
            TStructField tsf = preId2column.get(c.getColumn_id());
            destColumns.add(new Tuple2<String, TStructField>(c.getField_name(), tsf));
        }
        tDestCassandraSquid.setDestColumns(destColumns);

		TSquid preTSquid = ctx.getSquidOut(preSquids.get(0));
		tDestCassandraSquid.setPreTSquid(preTSquid);

		currentBuildedSquids.add(tDestCassandraSquid);
		return this.currentBuildedSquids;
	}
	
}
