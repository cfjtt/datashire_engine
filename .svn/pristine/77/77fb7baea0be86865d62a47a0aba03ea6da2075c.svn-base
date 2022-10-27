package com.eurlanda.datashire.engine.translation;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.adapter.db.AdapterDataSourceManager;
import com.eurlanda.datashire.engine.dao.SquidFlowDao;
import com.eurlanda.datashire.engine.entity.TColumn;
import com.eurlanda.datashire.engine.entity.TDataSource;
import com.eurlanda.datashire.engine.entity.TDatabaseSquid;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.engine.entity.clean.UpdateExtractLastValueInfo;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.exception.TargetSquidNotPersistException;
import com.eurlanda.datashire.engine.exception.TranslateException;
import com.eurlanda.datashire.engine.spark.DatabaseUtils;
import com.eurlanda.datashire.engine.translation.extract.ExtractManager;
import com.eurlanda.datashire.engine.translation.extract.SqlExtractManager;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.engine.util.StringUtils;
import com.eurlanda.datashire.engine.util.VariableUtil;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.StringUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;

/**
 * 数据库Extract翻译器。
 * 
 * @author Gene
 * 
 */
public class SqlExctractBuilder extends AbsTSquidBuilder implements TSquidBuilder {

	public SqlExctractBuilder(BuilderContext ctx, Squid currentSquid) {
		super(ctx, currentSquid);
	}

	@Override
	public List<TSquid> doTranslate(Squid cur) {
		// 数据库抽取
		ExtractSquid extractSquid = (ExtractSquid) cur;
		TDatabaseSquid ds = new TDatabaseSquid();
        // 设置 topN  ========= 2014-7-15 zdb ===========
        ds.setTopN(extractSquid.getTop_n());

		ds.setSquidId(cur.getId());
		// 因为extractSquid前面必定有一个datasourceSquid,直接取第一个
		DbSquid preSquids = (DbSquid) this.getPrevSquids().get(0);
		// extractSquid id
		ds.setId(extractSquid.getId() + ""); // TODO 保存映射关系 sId 对 squid
		ds.setConnectionSquidId(preSquids.getId());
		// 设置分割列信息
		ds.setSplitCol(buildSplitTColumn(extractSquid.getId(),
                extractSquid.getSource_table_id(), extractSquid.getSplit_col()));
		if(extractSquid.getSplit_num() == 0) {
			ds.setSplitNum(4);
		} else {
			ds.setSplitNum(extractSquid.getSplit_num());
		}
		//是否合表
		ds.setIs_union_table(extractSquid.getIsUnionTable());
		// 数据来源信息
		TDataSource dataSource = new TDataSource();
		dataSource.setDbName(preSquids.getDb_name());
		dataSource.setHost(preSquids.getHost());
		dataSource.setPassword(preSquids.getPassword());
		dataSource.setPort(preSquids.getPort());
		dataSource.setType(DataBaseType.parse(preSquids.getDb_type()));
		dataSource.setUserName(preSquids.getUser_name());
		//设置检查列类型
		if(extractSquid.isIs_incremental()){
			int checkCloumnId = extractSquid.getCheck_column_id();
			ReferenceColumn column = getSourceColumnById(extractSquid,checkCloumnId);
			if(column!=null){
			    dataSource.setCheckColumnType(column.getData_type());
            }
		}
		//判断是否需要合表
		boolean isUnionTable = extractSquid.getIsUnionTable();
		if(isUnionTable){
			int type = extractSquid.getTableNameSettingType();
			if(type==0){
				List<String> tableNames = JsonUtil.toGsonList(extractSquid.getTableNameSetting(),String.class);
				dataSource.setTableNames(new HashSet<>(tableNames));
			} else {
				Connection conn = DatabaseUtils.getConnection(dataSource);
				try {
					PreparedStatement stat = conn.prepareStatement(extractSquid.getTableNameSettingSql());
					ResultSet r = stat.executeQuery();
					Set<String> tableNames = new HashSet<>();
					while(r.next()){
						String name = r.getString(1);
						if(com.eurlanda.datashire.utility.StringUtils.isNotNull(name)){
							tableNames.add(name);
						}
					}
					dataSource.setTableNames(tableNames);
				} catch (SQLException e) {
					e.printStackTrace();
				} finally {
					try {
						conn.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
		} else {
			Set<String> tableNames = new HashSet<>();
			tableNames.add(getExtractSquidTableName(extractSquid));
			dataSource.setTableNames(tableNames);
		}
		//dataSource.getTableNames().add(getExtractSquidTableName(extractSquid));
		//对表名进行格式化 ``.``的形式，防止一些特殊的表名 ,只针对mysql，因为mysql支持特殊名字
		//规则为: 2n+1` aa 2n` bb 2n+1`
		if(DataBaseType.MYSQL==DataBaseType.parse(preSquids.getDb_type())){
            Set<String> tableNames = new HashSet<>();
			for(String tableName : dataSource.getTableNames()){
				String table = null;
				if(tableName.contains(".")){
					table = tableName.substring(tableName.indexOf(".")+1);
				} else {
					table = tableName;
				}
				table = "`"+table.replaceAll("`","``")+"`";
				if(tableName.contains(".")){
					tableName = tableName.substring(0,tableName.indexOf(".")+1)+table;
				} else {
					tableName = table;
				}
				tableNames.add(tableName);
			}
            dataSource.setTableNames(tableNames);
		}
		//规范teradata表名，加上""
		if(DataBaseType.TERADATA == DataBaseType.parse(preSquids.getDb_type())){
			Set<String> tableNames = new HashSet<>();
			for(String tableName : dataSource.getTableNames()){
				tableNames.add("\""+tableName+"\"");
			}
			dataSource.setTableNames(tableNames);
		}
		dataSource.setTableName(getExtractSquidTableName(extractSquid));
		if(preSquids.getDb_type()==DataBaseType.TERADATA.value()) {
			dataSource.setAlias("\""+cur.getName()+"\"");
		} else {
			dataSource.setAlias(cur.getName());
		}
		this.setFilter(extractSquid, dataSource);
		ds.setDataSource(dataSource);
        ds.setColumnSet(buildDataSquidTColumns(extractSquid));
        // 对hbase 需要表主键，将主键column id设为-1，hbase需要该主键来分页
        if(dataSource.getType() == DataBaseType.HBASE_PHOENIX) {
            ds.getColumnSet().addAll(buildDataSquidPKTColumns(extractSquid));
        }

		currentBuildedSquids.add(ds);
		TSquid df = super.doTranslateDataFall(extractSquid);
		if (df != null) {
			currentBuildedSquids.add(df);
		}
        // 增量/全量
        TSquid ret6 = doTranslateProcessMode(extractSquid);
        if (ret6 != null)
            currentBuildedSquids.add(ret6);
		return currentBuildedSquids;
	}

    /**
     * 生成分片列
     * @param extractSquidId extractsquid id
     * @param sourceTableId
     * @param splitColumnName  分片列名称
     * @return
     */
    public TColumn buildSplitTColumn(int extractSquidId,
            int sourceTableId, String splitColumnName) {
        SourceColumn column = getExtractSquidSourceColumns(extractSquidId,
                sourceTableId, splitColumnName);
        if(column == null) {
            return null;
        } else {
            TColumn c = new TColumn();
            c.setDbBaseDatatype(DbBaseDatatype.parse(column.getData_type()));
            c.setName(column.getName());
            return c;
        }
    }

	public void setFilter(ExtractSquid extractSquid, TDataSource tds) {
		tds.setParams(new ArrayList<Object>());
		/**
		 * 构造sql过滤器。 DBSource3.col1=123123 and DBSource3.col2= asdlfasdf
		 */
		String filterSQL = "";
		if (!ValidateUtils.isEmpty(extractSquid.getFilter())) {
			//List<Squid> prevSquids = this.getPrevSquids();
			String squidName = extractSquid.getName();
			filterSQL += extractSquid.getFilter().replace(squidName + ".", "");
		}

		// 替换其中的变量
		List<String> vaList = new ArrayList<>();
		filterSQL = StringUtil.ReplaceVariableForName(filterSQL, vaList);
		//变量
		for (String str : vaList) {
			// 重构变量的提取
			tds.getParams().add(VariableUtil.variableValue(str, ctx.getVariable(), extractSquid));
		}
		// 增量抽取。
		if (extractSquid.isIs_incremental()) {
			int incrementalMode = extractSquid.getIncremental_mode();
			//检查列
			int checkColumnId = extractSquid.getCheck_column_id();
			String lastValue = extractSquid.getLast_value();
			SquidFlowDao squidFlowDao = ConstantUtil.getSquidFlowDao();
			SourceColumn sourceColumn = squidFlowDao.getSourceColumnById(checkColumnId);
			if (sourceColumn == null) {
				throw new EngineException("元数据异常,没有找到对应的sourcecolumn,id:" + checkColumnId);
			}
			String checkColName = sourceColumn.getName();
			if (incrementalMode != 3) {
                //新增数据,最后修改时间
                ExtractManager extractManager = new SqlExtractManager(tds);
                try {
                	/*List<String> tableNames = new ArrayList<>();
                	boolean isUnionTable = extractSquid.getIsUnionTable();
                	if(isUnionTable){
                		tableNames.addAll(tds.getTableNames());
					}*/
					Integer limit = extractSquid.getMaxExtractNumberPerTimes();
                    //String[] filters = extractManager.genIncrementalFilterStringAndMaxValue(incrementalMode, tds.getTableName(), checkColName, lastValue,limit);
                    String[] filters = extractManager.genIncrementalFilterStringAndMaxValue(incrementalMode,tds.getTableNames(),checkColName,lastValue,limit);
					String filter = filters[0];
                    String newLastValue = filters[1];
                    if (com.eurlanda.datashire.utility.StringUtils.isNotEmpty(filter)) {
                        if (com.eurlanda.datashire.utility.StringUtils.isNotEmpty(filterSQL)) {
                            filterSQL += " and " + filter;
                        } else {
                            filterSQL += filter;
                        }
                    }
                    // 判断是否有增量,如果最新的值和上一次运行的值相同,则没有增量数据
                    if ((newLastValue != null && !newLastValue.equals(lastValue))) {
                        // 设置需要更新的lastValue值
                        tds.setUpdateLastValueInfo(
                                new UpdateExtractLastValueInfo(filters[1],
                                        extractSquid.getId()));
                    } else {
                        tds.setExistIncrementalData(false);
                    }
                } catch (Exception e){
                    e.printStackTrace();
                    throw e;
                } finally {
                    extractManager.close();
                }
            }  else if (incrementalMode == 3) {
				//使用原来的增量抽取方式
				if (extractSquid.isIs_incremental() && !ValidateUtils.isEmpty(extractSquid.getIncremental_expression())) {
					Matcher matcher = StringUtils.match(extractSquid.getIncremental_expression(), "([^\\.]*)\\s+>\\s+\\(Select Max\\((.*)\\)\\s+From\\s+([^\\.]*)\\.([^\\.]*)\\)");
					if (matcher.find()) {
						String colName = matcher.group(1);
						String condSquidName = matcher.group(3);
						String condColumnName = matcher.group(2);

						DataSquid condSquid = (DataSquid) this.ctx.getSquidByName(condSquidName);
						// 判断对应的squid 是否勾选了落地,没有勾选的直接异常
						if (!condSquid.isIs_persisted()) {
							throw new TranslateException("元数据异常,增量抽取表达式中选用的squid没有设置落地");
						}

						Integer destId = condSquid.getDestination_squid_id();
						final DbSquid dbSquid = (DbSquid) this.ctx.getSquidById(destId);

						DBConnectionInfo dbc = null;
						// 落地目标为默认，hbase数据库
						if (destId == 0 && dbSquid == null) {
							throw new TargetSquidNotPersistException(condSquid.getName());
						} else {
							dbc = DBConnectionInfo.fromDBSquid(dbSquid);
						}
						final DBConnectionInfo dbCon = dbc;
						String sql = "Select Max(" + condColumnName + ") as max_val From " + condSquid.getTable_name();
						Connection conn = null;
						Object max = null;
						try {
							conn = AdapterDataSourceManager.createConnection(dbCon);
							ResultSet rs = conn.createStatement().executeQuery(sql);
							if (rs.next()) {
								max = rs.getObject("max_val");
							}

						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							if (conn != null) {
								try {
									conn.close();
								} catch (SQLException e) {
									e.printStackTrace();
								}
							}
						}
						if (max != null) {
							if (!org.apache.commons.lang.StringUtils.isEmpty(filterSQL.trim())) {
								filterSQL += " and ";
							}
							filterSQL += colName + " > ?";
							tds.getParams().add(max);
						}
					}
				}
			}
		}
		tds.setFilter(org.apache.commons.lang.StringUtils.isEmpty(filterSQL) ? null : filterSQL);
	}

    public String getExtractSquidTableName(ExtractSquid squid){
        return getExtractSquidTableName(squid.getId(), squid.getSource_table_id());
    }

    public ReferenceColumn getSourceColumnById(ExtractSquid squid,int sourceColumnId){
        List<ReferenceColumn> refColumns = squid.getSourceColumns();
        ReferenceColumn refColumn = null;
        if(refColumns!=null){
            for(ReferenceColumn column : refColumns){
                if(column.getColumn_id()==sourceColumnId){
                    refColumn = column;
                    break;
                }
            }
        }
        return refColumn;
    }
}
