package com.eurlanda.datashire.engine.translation;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.adapter.db.AdapterDataSourceManager;
import com.eurlanda.datashire.engine.entity.TColumn;
import com.eurlanda.datashire.engine.entity.TDataSource;
import com.eurlanda.datashire.engine.entity.TDatabaseSquid;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.engine.util.ConfigurationUtil;
import com.eurlanda.datashire.engine.util.StringUtils;
import com.eurlanda.datashire.engine.util.cool.JList;
import com.eurlanda.datashire.entity.DBConnectionInfo;
import com.eurlanda.datashire.entity.DataSquid;
import com.eurlanda.datashire.entity.DbSquid;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.WebExtractSquid;
import com.eurlanda.datashire.entity.WebSquid;
import com.eurlanda.datashire.enumeration.DataBaseType;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Matcher;

import static com.eurlanda.datashire.engine.util.ConfigurationUtil.getInnerHbaseHost;
import static com.eurlanda.datashire.engine.util.ConfigurationUtil.getInnerHbasePort;

public class WebExtractBuilder extends AbsTSquidBuilder implements TSquidBuilder {

	public WebExtractBuilder(BuilderContext ctx, Squid currentSquid) {
		super(ctx, currentSquid);
	}

	@Override
	public List<TSquid> doTranslate(Squid cur) {
		// 现在默认为数据库抽取
		WebExtractSquid extractSquid = (WebExtractSquid) cur;
		TDatabaseSquid ds = new TDatabaseSquid();
		ds.setSquidId(cur.getId());
		// extractSquid id
		ds.setId(extractSquid.getId() + ""); // TODO 保存映射关系 sId 对 squid
		// 数据来源信息
		WebSquid preSquids = (WebSquid) this.getPrevSquids().get(0);
		ds.setConnectionSquidId(preSquids.getId());
		
		TDataSource dataSource = new TDataSource();
		dataSource.setType(DataBaseType.HBASE_PHOENIX);
		dataSource.setTableName("ds_spider.ed_web_content");
        dataSource.setHost(ConfigurationUtil.getInnerHbaseHost() + ":" + ConfigurationUtil.getInnerHbasePort());
//		dataSource.setPort(ConfigurationUtil.getInnerHbasePort());
		dataSource.setUserName("SA");
		dataSource.setPassword("");

		this.setFilter(extractSquid, dataSource);
		ds.setDataSource(dataSource);
		ds.setColumnSet(buildDataSquidTColumns(extractSquid));
        // 对hbase 需要表主键，将主键column id设为-1，hbase需要该主键来分页
        TColumn pkc = new TColumn();
        pkc.setId(-1);
        pkc.setPrimaryKey(true);
        pkc.setName("ID");
        // web 主键为 id
        ds.getColumnSet().add(pkc);

		currentBuildedSquids.add(ds);
		TSquid squid =super.doTranslateDataFall(extractSquid);
		if(squid!=null){
			currentBuildedSquids.add(squid);
		}
		return currentBuildedSquids;
	}

	public void setFilter(WebExtractSquid extractSquid, TDataSource tds) {

		/**
		 * 构造sql过滤器。 DBSource3.col1=123123 and DBSource3.col2= asdlfasdf
		 */
		String filterSQL = "";
		if (!ValidateUtils.isEmpty(extractSquid.getFilter())) {
			List<Squid> prevSquids = this.getPrevSquids();
			String squidName = prevSquids.get(0).getName();
			filterSQL += extractSquid.getFilter().replace(squidName + ".", "");
		}
//		JList<Object> cl = new JList<>();
//		tds.setParams(cl);
//		// 增量抽取。
//		if (!ValidateUtils.isEmpty(extractSquid.getIncremental_expression())) {
//			Matcher matcher = StringUtils.match(extractSquid.getIncremental_expression(), "^.*?\\.(.*?) > \\(Select Max\\((.*?)\\.(.*?)\\).*$");
//			String colName = matcher.group(1);
//			String condSquidName = matcher.group(2);
//			String condColumnName = matcher.group(3);
//
//			DataSquid condSquid = (DataSquid) this.ctx.getSquidByName(condSquidName);
//			Integer destId = condSquid.getDestination_squid_id();
//			final DbSquid dbSquid = (DbSquid) this.ctx.getSquidById(destId);
//			JDBCTemplate jdbc = new JDBCTemplate(new IConnectionProvider() {
//				@Override
//				public Connection getConnection() throws Exception {
//					return AdapterDataSourceManager.createConnection(DBConnectionInfo.fromDBSquid(dbSquid));
//				}
//			});
//			Object max = jdbc.queryForMap("Select Max(?) as max_val From ?", condColumnName, condSquid.getTable_name()).get("max_val");
//			filterSQL += " and " + colName + " > ?";
//			cl.a(max);
//
//		}

        // 增量抽取。
        if (!ValidateUtils.isEmpty(extractSquid.getIncremental_expression())) {
            Matcher matcher = StringUtils.match(extractSquid.getIncremental_expression(), "([^\\.]*)\\s+>\\s+\\(Select Max\\((.*)\\)\\s+From\\s+([^\\.]*)\\.([^\\.]*)\\)");
            if(matcher.find()) {
                String colName = matcher.group(1);
                String condSquidName = matcher.group(3);
                String condColumnName = matcher.group(2);

                DataSquid condSquid = (DataSquid) this.ctx.getSquidByName(condSquidName);
                Integer destId = condSquid.getDestination_squid_id();
                final DbSquid dbSquid = (DbSquid) this.ctx.getSquidById(destId);
                DBConnectionInfo dbc = null;
                // 落地目标为默认，hbase数据库
                if(destId == 0 && dbSquid == null) {
                    dbc = new DBConnectionInfo();
                    dbc.setDbType(DataBaseType.HBASE_PHOENIX);
                    dbc.setHost(getInnerHbaseHost());
                    dbc.setPort(getInnerHbasePort());
                } else {
                    dbc = DBConnectionInfo.fromDBSquid(dbSquid);
                }
                final DBConnectionInfo dbCon = dbc;

//                JDBCTemplate jdbc = new JDBCTemplate(new IConnectionProvider() {
//                    @Override
//                    public Connection getConnection() throws Exception {
//                        return AdapterDataSourceManager.createConnection(dbCon);
//                    }
//                });
                String sql = "Select Max(" + condColumnName +") as max_val From " + condSquid.getTable_name();
//                Object max = jdbc.queryForMap(sql).get("max_val");
                Connection conn = null;
                Object max = null;
                try {
                    conn = AdapterDataSourceManager.createConnection(dbCon);
                    ResultSet rs = conn.createStatement().executeQuery(sql);
                    if(rs.next()) {
                        max = rs.getObject("max_val");
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if(conn != null) {
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
//                Object max = .createStatement().executeQuery(sql)
                if(max != null) {
                    filterSQL += " and " + colName + " > ?";
                    tds.setParams(JList.create(max));
                }
            }
        }

		filterSQL+=" and squidId="+extractSquid.getId()+" and repositoryId="+this.ctx.getRepositoryId();
		tds.setFilter(" 1=1 " + filterSQL);
	}

}
