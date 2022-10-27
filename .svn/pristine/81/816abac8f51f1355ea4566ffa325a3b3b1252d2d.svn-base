package com.eurlanda.datashire.engine.translation;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.engine.entity.*;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.exception.EngineExceptionType;
import com.eurlanda.datashire.engine.spark.squid.TJoinSquid;
import com.eurlanda.datashire.engine.util.StringUtils;
import com.eurlanda.datashire.engine.util.cool.JList;
import com.eurlanda.datashire.engine.util.cool.JMap;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.JoinType;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.server.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;

/**
 * 处理stage.
 * 
 * @author Gene
 */
public class StageBuilder extends AbsTSquidBuilder implements TSquidBuilder {
	protected static Logger logger = LoggerFactory.getLogger(StageBuilder.class);

	protected Map<Integer, Integer> squidDropMapper = new HashMap();
	protected List<TransformationLink> preparedLinks = new ArrayList<TransformationLink>();
	protected Map<Integer, TTransformation> trsCache = new JMap<>();

    protected List<Map<Integer, TStructField>> id2Columns = null;
	private static final ExecutorService executor = Executors.newCachedThreadPool();

	public StageBuilder(BuilderContext ctx, Squid currentSquid) {
		super(ctx, currentSquid);
	}

	@Override
	public List<TSquid> doTranslate(Squid cur) {
		StageSquid stageSquid = (StageSquid) cur;

		List<TSquid> ret1 = doTranslateJoin(stageSquid);
		if (ret1 != null) {
            for(TSquid rs : ret1) {
                rs.setSquidId(cur.getId());
                rs.setName(stageSquid.getName());
                currentBuildedSquids.add(rs);
            }
        }

		TSquid ret11 = doTranslateNewSquidFilter(stageSquid);
		if (ret11 != null) {
            ret11.setSquidId(cur.getId());
            currentBuildedSquids.add(ret11);
        }

		TSquid ret2 = doTranslateTransformations(stageSquid);
		if (ret2 != null) {
            ret2.setSquidId(cur.getId());
            currentBuildedSquids.add(ret2);
        }

		TSquid ret3 = doTranslateAggregate(stageSquid);
		if (ret3 != null) {
            ret3.setSquidId(cur.getId());
            currentBuildedSquids.add(ret3);
        }

		TSquid ret4 = doTranslateDistinct(stageSquid);
		if (ret4 != null) {
            ret4.setSquidId(cur.getId());
            currentBuildedSquids.add(ret4);
        }

		TSquid ret5 = doTranslateDataFall(stageSquid);
		if (ret5 != null) {
            ret5.setSquidId(cur.getId());
            currentBuildedSquids.add(ret5);
        }

		TSquid ret6 = doTranslateProcessMode(stageSquid);
		if (ret6 != null) {
            ret6.setSquidId(cur.getId());
            currentBuildedSquids.add(ret6);
        }

		return this.currentBuildedSquids;
	}

	protected TSquid doTranslateDistinct(DataSquid stageSquid) {
		if (stageSquid.getIs_distinct() == 0) {
			TDistinctSquid tds = new TDistinctSquid();
			tds.setSquidId(stageSquid.getId());
			tds.setName(stageSquid.getName());
			tds.setTaskId(this.ctx.getTaskId());
			tds.setPreviousSquid(this.getCurLastTSqud());
			return tds;
		}
		return null;
	}

	public TSquid doTranslateAggregate(Squid squid) {

		DataSquid stageSquid = (DataSquid) squid;

        TranslateUtil.AggregateInfo aggregateInfo = TranslateUtil.translateAggregateInfo(stageSquid);

        if(aggregateInfo == null) {
            return null;
        }

        // group by 的列
        List<Integer> groupKeyList = aggregateInfo.getGroupKeyList();
        // 聚合操作
        List<AggregateAction> aaList = aggregateInfo.getAaList();

		TAggregateSquid tAggregateSquid = null;
		if (!groupKeyList.isEmpty() || aaList.size()>0) {
			tAggregateSquid = new TAggregateSquid();
			tAggregateSquid.setGroupKeyList(groupKeyList);
			tAggregateSquid.setAaList(aaList);
			tAggregateSquid.setSquidId(stageSquid.getId());
			tAggregateSquid.setPreviousSquid(this.getCurLastTSqud());
			tAggregateSquid.setName(stageSquid.getName());
			logger.debug("[聚合变换]\t" + groupKeyList);
			return tAggregateSquid;
		} else {
			logger.debug("[聚合变换] 当前stage没有聚合变换.");
		}

		return null;
	}

	/**
	 * 转换join
	 * 
	 * @param stageSquid
	 * @return
	 */
	public List<TSquid> doTranslateJoin(StageSquid stageSquid) {
		List<TSquid> ret = null;
		// 判断是否有 join 部分
		if (stageSquid == null || stageSquid.getJoins() == null || stageSquid.getJoins().size() < 2) {
			logger.debug(stageSquid.getSquidflow_id() + "_" + stageSquid.getId() + " 不存在 join");
			return null;
		} else {
			List<SquidJoin> squidJoins = stageSquid.getJoins();
			// 按照 prior_join_id 增序
			Collections.sort(squidJoins, new Comparator<SquidJoin>() {
				@Override
				public int compare(SquidJoin o1, SquidJoin o2) {
					return o1.getPrior_join_id() - o2.getPrior_join_id();
				}
			});
			// 判断是 TJoinSquid 还是 TUnionSquid
			if (squidJoins.get(1).getJoinType() > JoinType.BaseTable.value() && squidJoins.get(1).getJoinType() < JoinType.Unoin.value()) {
				// 翻译joinSquid 返回多个Join后的 Squid
				//ret = (translateJoinSquid(squidJoins, stageSquid));
				ret = translateJoinSquid2SJoinSquid(squidJoins, stageSquid);
			} else {
				// 翻译 unionSquid
				ret = translateUnionSquid(squidJoins);
			}
        }
		return ret;
	}


	/**
	 * 取得filter表达式中变量或者常量对应的dataCell.
	 * @param val
	 * @return 
	 */
    /**
	private Integer lookupVarKey(String val){
		
		 if(val.startsWith("$")){  		// transformation, 从Trs过滤表达式中取依赖的Trs的outKey. 
			Transformation tf = this.getTranByName(val.replace("$", ""));
			TTransformation ttf = this.trsCache.get(tf.getId());
			List<Integer> outKeys = ttf.getOutKeyList();
			Integer outKey =  outKeys.get(0);
			String index = StringUtils.find(val, "\\[(\\d+)\\]", 1);
			if(!ValidateUtils.isEmpty(index)){
				outKey= outKeys.get(Integer.parseInt(index));
			}
			return outKey;
		}else{		//默认为referenceColumn. 语法为 SquidName.columnName
			if(val.contains("$")){
				DataSquid ds = (DataSquid) this.currentSquid;
				String squidName = val.split("\\$")[0];
				String colName= val.split("\\$")[1];
				
				for(ReferenceColumn col:ds.getSourceColumns()){
					if(colName.equals(col.getName()) && squidName.equals(col.getGroup_name())){
						return -col.getColumn_id();
					}
				}
			}
			throw new RuntimeException("未找到变量"+val+"对应的referenceColumn.");
		}
	}
     */
	/**
	 * 将过滤字符串转换成filter类。 a>1 and b<2 and c=5 or dd=3 ad
	 * 
	 * @param filterString
	 *            过滤字符串
	 * @return
	 */
    /**
	public TFilterExpression translateTrsFilter(String filterString, Squid squid) {
		if(ValidateUtils.isEmpty(filterString)){
			return null;
		}
		logger.debug("解析Transformation 过滤表达式:{}",filterString);
		String fs = filterString.replaceAll("(?<=[^><=!])=(?=[^><=])", " == ")
				.replaceAll("\\s(?i)and", " && ")
				.replaceAll("\\s(?i)or", " || ")
				.replaceAll("#|(\\w+)\\.(?!\\d+)", "$1\\$")
				.replace("'", "\"")
				.replace("<>", "!=");
		
		// 建立变量映射。查找变量
		Matcher  matcher = StringUtils.match(fs, "(\\w+\\$\\w+)|(\\$\\w+)");
		Map<String,Integer> keyMap = new HashMap<String, Integer>();
		while(matcher.find()){
			String var = matcher.group();
			Integer key= lookupVarKey(var);
			keyMap.put(var, key);
		}
		// 预处理时间，将时间转换成long 类型。
		Matcher m = StringUtils.match(fs, "'([\\d\\:\\-\\s\\.]+?)'");
		String tmp = fs;
		while (m.find()) {
			String expVal = m.group(1);
			Date date = null;
			if (!ValidateUtils.isNumeric(expVal)) {
				try {
					date = DateTimeUtils.parserDateTime(expVal, "yyyy-MM-dd hh:mm:ss.SSS");
				} catch (Exception e3) {
					try {
						date = DateTimeUtils.parserDateTime(expVal, "yyyy-MM-dd hh:mm:ss");
					} catch (Exception e2) {
						try {
							date = DateTimeUtils.parserDateTime(expVal, "yyyy-MM-dd");
						} catch (Exception e) {
							throw new RuntimeException("不支持的日期格式："+expVal);
						}

					}
				}
				
			}
			if (date != null) {
				tmp = tmp.replace(m.group(),date.getTime() + "");
			}
		}
		TFilterExpression tfe = new TFilterExpression();
		// 查找表达式中的变量， @name
		List<String> varList = new ArrayList<>();
		tmp = StringUtil.replaceVariableForNameToJep(tmp, varList);
		Map<String, Object> varMap = new HashMap<>();
		for(String str : varList) {
			varMap.put("$"+str, VariableUtil.variableValue(str, ctx.getVariable(), squid));
		}
		tfe.setVarMap(varMap);
		tfe.setExpression(tmp);

		tfe.setKeyMap(keyMap);
		
		return tfe;
	}
     */

	protected List<TSquid> translateUnionSquid(List<SquidJoin> squidJoins) {
        List<TSquid> squids = new ArrayList<>();
		// 左边的tsquid
		TSquid leftTSquid = null;
		for (SquidJoin squidJoin : squidJoins) {
			// 多路jion 会翻译成 n-1个TJoinSquid
			if (squidJoin.getJoinType() == JoinType.BaseTable.value()) {
				try {
					leftTSquid = ctx.getSquidOut(squidJoin.getJoined_squid_id());
                    // 设置union的 id2columns
                    id2Columns = new ArrayList<>();
                    id2Columns.add(TranslateUtil.getId2ColumnsFromSquid(this.ctx.getSquidById(squidJoin.getJoined_squid_id())));
				} catch (Exception e) {
					throw new RuntimeException("未找到参加join的squid，id:" + squidJoin.getJoined_squid_id());
				}
			} else {
				TUnionSquid tUnionSquid = new TUnionSquid();
				tUnionSquid.setLeftSquid(leftTSquid);
				tUnionSquid.setRightSquid(ctx.getSquidOut(squidJoin.getJoined_squid_id()));

				TUnionType tut = null;
				if (squidJoin.getJoinType() == JoinType.Unoin.value()) {
					tut = TUnionType.UNION;
				} else if (squidJoin.getJoinType() == JoinType.UnoinAll.value()) {
					tut = TUnionType.UNION_ALL;
				} else {
					logger.error("uion 类型不匹配：" + squidJoin.getJoinType());
					throw new RuntimeException("uion 类型不匹配：" + squidJoin.getJoinType());
				}
				tUnionSquid.setUnionType(tut);

				// 设置 inkeyList,outKeyList
				// inKey 为 rightSquid的输入Keylist;outKey为leftSquid的输入keyList
				DataSquid rds = (DataSquid) ctx.getSquidById(squidJoin.getJoined_squid_id());
				List<Column> rcs = rds.getColumns();
				// 第一个 base
				DataSquid lds = (DataSquid) ctx.getSquidById(squidJoins.get(0).getJoined_squid_id());
				List<Column> lcs = lds.getColumns();

				// 现在采用的潜规则： 两个数据集进行uion ,列数必须一样，数据类型一致
				if (rcs.size() != lcs.size()) {
					logger.error("非法uion,两个数据集列数不一样");
					throw new EngineException(EngineExceptionType.UNION_COLUMN_SIZE_IS_NOT_EQUALS);
				}

				// 排序
				Collections.sort(rcs, new Comparator<Column>() {
					@Override
					public int compare(Column o1, Column o2) {
						return o1.getRelative_order() - o2.getRelative_order();
					}
				});
				Collections.sort(lcs, new Comparator<Column>() {
					@Override
					public int compare(Column o1, Column o2) {
						return o1.getRelative_order() - o2.getRelative_order();
					}
				});

				List<Integer> rKeyList = new ArrayList<>();
				for (Column c : rcs) {
					rKeyList.add(c.getId());
				}
				List<Integer> lKeyList = new ArrayList<>();
				for (Column c : lcs) {
					lKeyList.add(c.getId());
				}
				tUnionSquid.setLeftInKeyList(lKeyList);
				tUnionSquid.setRightInKeyList(rKeyList);
				// tUnionSquid.setOutKeyList(outKeyList);//leftInKey is
				// outKeyList
                squids.add(tUnionSquid);
				leftTSquid = tUnionSquid;
			}
		}
		return squids;
	}

	/**
	 * 翻译TJoinSquid
	 * 
	 * @param squidJoins
	 * @return

	protected List<TSquid> translateJoinSquid(List<SquidJoin> squidJoins, StageSquid stageSquid) {
		// joinSquid
        List<TSquid> squids = new ArrayList<>();
		Set<String> leftSquidNames = new HashSet<>();
		TSquid leftTSquid = null;
		for (SquidJoin squidJoin : squidJoins) {
			// 多路join 会翻译成 n-1个TJoinSquid
			if (squidJoin.getJoinType() == JoinType.BaseTable.value()) {
				leftSquidNames.add(ctx.getSquidById(squidJoin.getJoined_squid_id()).getName());
				leftTSquid = ctx.getSquidOut(squidJoin.getJoined_squid_id());
				continue;
			} else {
				TJoinSquid1 tJoinSquid1 = new TJoinSquid1(); //
				SquidJoin baseSJ2 = squidJoin; // 右边第二个
				// joinType
				tJoinSquid1.setJoinType(JoinType.parse(baseSJ2.getJoinType()));
				tJoinSquid1.setLeftSquid(leftTSquid);
				tJoinSquid1.setRightSquid(ctx.getSquidOut(baseSJ2.getJoined_squid_id()));

				// 解析join连接字符
                // 判断join条件是否为空
                if(org.apache.commons.lang.StringUtils.isEmpty(baseSJ2.getJoin_Condition())) {
                    throw new RuntimeException("squidflow 数据异常， join条件不能为空");
                }
				String jcString = baseSJ2.getJoin_Condition().replaceAll(" and ", " AND ");
				String[] jcs = jcString.split(" AND ");
				List<TJoinCondition> jcList = new ArrayList<>();
				for (String jc : jcs) {
					if (jc.trim().isEmpty()) {
						continue;
					} else {
						// 处理 squidName.columnName = squidName.columnName
						String[] paths = jc.split("=");
						if (paths.length != 2) {
							logger.error("join表达式非法，" + jc);
							throw new RuntimeException("join表达式非法，" + jc);
						} else {
							// 处理 squidName.columnName 分别设置到对应的 leftKey 或者
							// rightKey中
							TJoinCondition tjc = new TJoinCondition();
							for (int i = 0; i < paths.length; i++) {
								String path = paths[i];
								String[] parts = path.split("[.]");
								int pathsLength = parts.length;
								if (parts.length < 2) {
									logger.error("join表达式非法，" + jc);
									throw new RuntimeException("join表达式非法，" + jc);
								} else {
									String squidName = parts[pathsLength - 2].trim();
									String columnName = parts[pathsLength - 1].trim();
									// 获得表达式所指的 referenceColumn
									ReferenceColumn rc = getRCBySquidNameAndColumnName(squidName, columnName, stageSquid);
									// 根据squidName获取Squid
									Squid s = ctx.name2squidMap.get(squidName);
									// 根据referenceColumn获取对应数据的key
									int columnId = rc.getColumn_id();
									if (leftSquidNames.contains(squidName)) {
										tjc.setLeftKeyId(columnId);
									} else if (ctx.getSquidById(baseSJ2.getJoined_squid_id()).getName().equals(squidName)) {
										tjc.setRightKeyId(columnId);
									} else {
										throw new RuntimeException("join表达式非法，" + jc + " .... 对应的lefKey rightKey 不匹配");
									}

								}
							}
							// 添加 jion 条件
							jcList.add(tjc);
						}
					}
				}
				// 设置join 条件
				tJoinSquid1.setJoinConditionList(jcList);

				// 将当前join的squid,加入到leftSquids
				// leftSquidNames.add(getSquidById(baseSJ2.getJoined_squid_id()).getName());
				leftSquidNames.add(ctx.getSquidById(baseSJ2.getJoined_squid_id()).getName());

				// 设置输入、输出列 暂时是全部输入，全部输出，columnid不变,
				// TODO 下一步可以根据transformationlink 来决定哪些输入、输出。
				// TODO（有可能没有Link链出，但是joinCondition中用到，需要时间，暂不做）n
				tJoinSquid1.setInKeyList(getOutKeyListFromSquidNames(leftSquidNames));
				tJoinSquid1.setOutKeyList(getOutKeyListFromSquidNames(leftSquidNames));
                squids.add(tJoinSquid1);
				leftTSquid = tJoinSquid1;
			}
		}
		return squids;
	} */

	/**
	 * 翻译TJoinSquid
	 *
	 * @param squidJoins
	 * @return
	 */
	protected List<TSquid> translateJoinSquid2SJoinSquid(List<SquidJoin> squidJoins, StageSquid stageSquid) {
		// joinSquid
		List<TSquid> squids = new ArrayList<>();

//		List<RDD<Map<Integer, DataCell>>> rdds = new ArrayList<>();
		List<TSquid> preTSquids = new ArrayList<>();
		List<JoinType> joinTypes = new ArrayList<>();
		List<String> joinExpressions = new ArrayList<>();
		List<String> squidNames = new ArrayList<>();
		for (SquidJoin squidJoin : squidJoins) {

			// 获取当前参与Join 的squid
			Squid squid = ctx.getSquidById(squidJoin.getJoined_squid_id());
			//rdds.add(ctx.getSquidOut(squidJoin.getJoined_squid_id()).getOutRDD().rdd());
			preTSquids.add(ctx.getSquidOut(squidJoin.getJoined_squid_id()));
			joinTypes.add(JoinType.parse(squidJoin.getJoinType()));
			joinExpressions.add(squidJoin.getJoin_Condition());
			squidNames.add(squid.getName());

		}

        // 将这个值设置为成员变量, 留着给filter使用
        this.id2Columns = TranslateUtil.getId2ColumnsFormStageSquid(squidJoins, ctx);
		TJoinSquid
                tJoinSquid = new TJoinSquid(preTSquids, joinTypes, joinExpressions, squidNames, id2Columns);
		tJoinSquid.setId(stageSquid.getId() + "");
		tJoinSquid.setType(TSquidType.JOIN_SQUID);
		squids.add(tJoinSquid);
		return squids;
	}

    public List<Map<Integer, TStructField>> getOrGenId2Columns() {
        if (this.id2Columns == null) { // 为空的情况:该stagesquid 没有join
            id2Columns = new ArrayList<>();
            id2Columns.add(TranslateUtil.getId2ColumnsFromSquid(this.ctx.getSquidById(getPrevSquids().get(0).getId())));
        }
        return this.id2Columns;
    }

	/**
	 * 从 stageSquid中根据squidName,columnName 获得referenceColumn
	 * 
	 * @param squidName
	 * @param columnName
	 * @param stageSquid
	 * @return
	 */
	protected ReferenceColumn getRCBySquidNameAndColumnName(String squidName, String columnName, StageSquid stageSquid) {
		Squid s = this.ctx.getSquidByName(squidName);
		if (s == null) {
			throw new RuntimeException(String.format("此名字[%s]对应的squid不存在!", squidName));
		}
		List<Column> cList = TranslateUtil.getColumnsFromSquid(s);

		List<ReferenceColumn> rcList = stageSquid.getSourceColumns();
		Column sourceColumn = null;
		for (Column c : cList) {
			if (columnName.equals(c.getName())) {
				sourceColumn = c;
			}
		}
		if (sourceColumn != null) {
			for (ReferenceColumn rc : rcList) {
				if (rc.getColumn_id() == sourceColumn.getId()) {
					return rc;
				}
			}
		}
		throw new RuntimeException("Join数据异常，" + stageSquid.getName() + "中不能匹配到 " + squidName + "." + columnName);
	}

	/**
	 * 根据squidName获取对应Squid的输出columnId
	 * 
	 * @param squidNames
	 * @return
	 */
	protected List<Integer> getOutKeyListFromSquidNames(Set<String> squidNames) {
		List<Integer> outList = new ArrayList<>();
		// 根据squidName -> squid -> columns -> columnId
		for (String name : squidNames) {
			Squid ds = ctx.name2squidMap.get(name);
			List<Column> cs = TranslateUtil.getColumnsFromSquid(ds);
			for (Column c : cs) {
				// Todo:Juntao.Zhang delete hard code
				if (!c.getName().equals(Constants.DEFAULT_EXTRACT_COLUMN_NAME))
					outList.add(c.getId());
			}
		}

		return outList;
	}

	public TSquid doTranslateTransformations(Squid squid) {
		TTransformationSquid ts = new TTransformationSquid();
		ts.setSquidId(squid.getId());
		ts.setId(squid.getId() + "");
		List<TTransformationAction> actions = new StageTransformationActionTranslator((DataSquid) squid, ctx.getIdKeyGenerator(), ctx.getVariable(), ctx, ts).translateTTransformationActions();
		ts.settTransformationActions(actions);
		TSquid prevTSquid = this.getCurLastTSqud();
		if (prevTSquid == null) {
			List<Squid> prevs = this.ctx.getPrevSquids(squid);
			if(prevs.size()==0) throw new RuntimeException("未找到本squid的前置squid.");
			Squid prevSquid = prevs.get(0);
			prevTSquid = this.ctx.getSquidOut(prevSquid);
		}
		ts.setName(squid.getName());
		ts.setPreviousSquid(prevTSquid);
		return ts;
	}

	/**
	 * 没有连接exceptionSquid时，对transformation进行剪枝操作
	 */
	private int cutTransWithNoExceptionSquid() {
		DataSquid ss = (DataSquid) this.currentSquid;
		List<Transformation> trans = ss.getTransformations();
		List<Transformation> removes = new ArrayList<>();
		Column idColumn = null;
		for(Column c : ss.getColumns()) {
			if("id".equals(c.getName())) {
				idColumn = c;
			}
		}
		for (Transformation tf : trans) {
			if(tf.getTranstype() == TransformationTypeEnum.VIRTUAL.value()) {
				List<TransformationLink> toLinks = getLinksToTran(tf);
				if(toLinks.size() == 0) {
					List<TransformationLink> fromLinks = getLinksFromTran(tf);
					if(fromLinks.size() == 0) {
						// 排除ID COLUMN的列
						if(idColumn != null && tf.getColumn_id() != idColumn.getId()) {
							removes.add(tf);
						}
					}
				}
				continue;
			}
			// 找到是否有link从该transformation连出，
			// 1.有则说明这个transformation存在下游导出，
			// 2.没有则需要将该transformation删除，并且删除所有连入到该transformation的link
			List<TransformationLink> fromlinks = getLinksFromTran(tf);
			if (fromlinks.size() == 0) {
				// 删除所有连入到该transformation的link
				List<TransformationLink> tolinks = getLinksToTran(tf);
				this.preparedLinks.removeAll(tolinks);
				// 删除该transformation
				removes.add(tf);
			}
		}
		trans.removeAll(removes);
		return removes.size();
	}

	/**
	 * 对transformation进行剪枝操作
	 */
	private int cutTrans() {
		DataSquid ss = (DataSquid) this.currentSquid;
		List<Transformation> trans = ss.getTransformations();
		List<Transformation> removes = new ArrayList<>();
		for (Transformation tf : trans) {
			if(tf.getTranstype() == TransformationTypeEnum.VIRTUAL.value()) {
				continue;
			}
			// 找到是否有link从该transformation连出，
			// 1.有则说明这个transformation存在下游导出，
			// 2.没有则需要将该transformation删除，并且删除所有连入到该transformation的link
			List<TransformationLink> fromlinks = getLinksFromTran(tf);
			if (fromlinks.size() == 0) {
				// 删除所有连入到该transformation的link
				List<TransformationLink> tolinks = getLinksToTran(tf);
				this.preparedLinks.removeAll(tolinks);
				// 删除该transformation
				removes.add(tf);
			}
		}
		trans.removeAll(removes);
		return removes.size();
	}

	private List<TransformationLink> getLinksToTran(Transformation transformation) {
		List<TransformationLink> links = new ArrayList<>();
		for(TransformationLink tl : this.preparedLinks) {
			if(tl.getTo_transformation_id() == transformation.getId()) {
				links.add(tl);
			}
		}
		return links;
	}

	private List<TransformationLink> getLinksFromTran(Transformation transformation) {
		List<TransformationLink> links = new ArrayList<>();
		for(TransformationLink tl : this.preparedLinks) {
			if(tl.getFrom_transformation_id() == transformation.getId()) {
				links.add(tl);
			}
		}
		return links;
	}

	/**
	 * 检查翻译ID自增列。
	 * @return 

	protected TTransformationAction buildIDAction() {
		if(this.currentSquid instanceof DataMiningSquid){
			return null;
		}
		
		DataSquid condSquid = (DataSquid) this.currentSquid;
		
		Column idCol = null;
		for (Column col : condSquid.getColumns()) {
			if (col.getName().toLowerCase().equals("id")) {
				idCol= col;
				break;
			}
		}
		if(idCol==null) return null;
		
		Transformation tf = this.getTranByColumnId(idCol.getId());
		List<Transformation> inTrs = this.getInTrs(tf);				// 如果Transformation有连线就跳过。非系统ID
		if(inTrs.size()>0){
			return null;
		}
		
		
		Integer destId = condSquid.getDestination_squid_id();
		Integer max = 0;
		if(condSquid.isIs_persisted() && destId!=null){
			if(idCol==null) return null;

			// 求现有落地表中最大的ID值
			DBConnectionInfo dbci = null;
			if(destId==0) {
				// 内部默认hbase
				dbci = TranslateUtil.getDefaultDBConnectionInfo();
			} else if(destId>0) {
				Squid dbs = this.ctx.getSquidById(destId);
				if(dbs instanceof DbSquid) {
					DbSquid dbSquid = (DbSquid) this.ctx.getSquidById(destId);
					dbci = DBConnectionInfo.fromDBSquid(dbSquid);
					final DBConnectionInfo dbConnectionInfo = dbci;
					JDBCTemplate jdbc = new JDBCTemplate(new IConnectionProvider() {
						@Override
						public Connection getConnection() throws Exception {
							return AdapterDataSourceManager.createConnection(dbConnectionInfo);
						}
					});
					max = jdbc.queryForInt("Select Max("
							+ TColumn.getName(dbConnectionInfo.getDbType(), idCol.getName())
							+ ") as max_val From " + condSquid.getTable_name());
				} else if(dbs instanceof NOSQLConnectionSquid) {
					NOSQLConnectionSquid nosqlConnectionSquid = (NOSQLConnectionSquid)dbs;
					if(nosqlConnectionSquid.getDb_type() == NoSQLDataBaseType.MONGODB.value()) {
						DB db = NoSqlConnectionUtil.createMongoDBConnection(nosqlConnectionSquid);
						DBCursor dbCursor =
    db.getCollection(condSquid.getTable_name()).find(new BasicDBObject(idCol.getName(), 1)).sort(new BasicDBObject(idCol.getName(), -1)).limit(1);
						if(dbCursor.hasNext()) {
							DBObject dbObject = dbCursor.next();
							if (dbObject != null && dbObject.get(idCol.getName()) != null) {
								max = Integer.parseInt(dbObject.get(idCol.getName()).toString());
							}
						}
					}
				}
			}

		}
		int maxVal = (int) (max==null?0:max);
		
		TTransformationAction action = new TTransformationAction();
		TTransformation ttf = new TTransformation();
		ttf.setType(TransformationTypeEnum.AUTO_INCREMENT);
		ttf.setInKeyList(new JList<Integer>(0));
		ttf.setOutKeyList(new JList<Integer>(idCol.getId()));
		ttf.setDbId(tf.getId());
		Map<String,Object> infoMap = new HashMap<String, Object>();
		infoMap.put(TTransformationInfoType.ID_AUTO_INCREMENT_MIN.dbValue,maxVal-1);
		ttf.setInfoMap(infoMap);
		ttf.setName(tf.getName());
		action.settTransformation(ttf);
		
		return action;
		
	}*/

	/**
	 * 待完成
	 * 
	 * @param tf
	 * @return

	protected TTransformationAction buildAction(TTransformationSquid tsquid,Transformation tf, Squid cursquid) {
		
		//检查本transformation是否满足运行条件。
		List<Transformation> intrs = this.getInTrs(tf);
		for(Transformation x: intrs){
			if(!trsCache.containsKey(x.getId())){
				logger.debug("transformation不满足编译条件，退出。");
				return null;
			}
		}
		
		TTransformationAction action = new TTransformationAction();
		TTransformation ttf = new TTransformation();
		// todo: 需要确定每个transformation 的参数。
		// ttf.putInfo(tf.get);
		TransformationTypeEnum typeEnum = null;
		try {
			typeEnum = TransformationTypeEnum.valueOf(tf.getTranstype());
			ttf.setType(typeEnum);
		} catch (EnumException e) {
			e.printStackTrace();
		}
		ttf.setOutKeyList(genOutKeyList(tf));
		
//		ttf.setFilterExpression(translateTrsFilter(tf.getTran_condition(), cursquid));
		ttf.setFilterExpression(TranslateUtil.translateTransformationFilter(tf.getTran_condition(), (DataSquid) cursquid, this.trsCache, ctx.getVariable()));
		ttf.setInputsFilter(getFilterList(tf, cursquid));
		if(!isConstantTrs(tf)){
			ttf.setInKeyList(getInkeyList(tf));
		}
		ttf.setName(tf.getName());
		ttf.setDbId(tf.getId());
		Map<String, Object> infoMap = ClassUtils.bean2Map(tf);
		// 转换变量的值
        TranslateUtil.convertVariable(infoMap, ctx.getVariable(), cursquid);
		infoMap.put(TTransformationInfoType.SQUID_FLOW_ID.dbValue, this.ctx.squidFlow.getId());
		infoMap.put(TTransformationInfoType.SQUID_FLOW_NAME.dbValue, this.ctx.squidFlow.getName());
		infoMap.put(TTransformationInfoType.PROJECT_ID.dbValue, this.ctx.squidFlow.getProject_id());
		infoMap.put(TTransformationInfoType.PROJECT_NAME.dbValue,this.ctx.project.getName());
		infoMap.put(TTransformationInfoType.TASK_ID.dbValue,this.ctx.getTaskId());
		infoMap.put(TTransformationInfoType.JOB_ID.dbValue, this.ctx.jobId);

		// 对infoMap中可以出现变量的值进行转换


		if(this.currentSquid instanceof DataMiningSquid && typeEnum.equals(TransformationTypeEnum.VIRTUAL)){
			infoMap.put(TTransformationInfoType.SKIP_VALIDATE.dbValue, true);
		}
		if (tf.getDictionary_squid_id() != 0) {
			Integer columnId = 0;
			Squid squid = this.ctx.getSquidById(tf.getDictionary_squid_id());
			if (squid instanceof DocExtractSquid) {
				DocExtractSquid docExt = (DocExtractSquid) squid;
				columnId = docExt.getColumns().get(0).getId();
			} else if (squid instanceof DataSquid) {
				columnId = ((DataSquid) squid).getColumns().get(0).getId();
			}

			TSquid ts = this.ctx.getSquidOut(squid);
			infoMap.put(TTransformationInfoType.DICT_SQUID.dbValue, ts);
			infoMap.put(TTransformationInfoType.DICT_COLUMN_KEY.dbValue, columnId);
		}
		// 训练数据
		if(typeEnum.equals(TransformationTypeEnum.TRAIN)){
			infoMap.put(TTransformationInfoType.PREDICT_MODEL_TYPE.dbValue,SquidTypeEnum.parse(this.currentSquid.getSquid_type()));
			
		}else if(typeEnum.equals(TransformationTypeEnum.PREDICT) || 
				typeEnum.equals(TransformationTypeEnum.INVERSEQUANTIFY) ){		// 预测模型。
			int predictSquidId = tf.getModel_squid_id();
			// 拿到预测模型。
			Map<String,Object> dm_squid=getCurJdbc().queryForMap("select dm.*,s.* from ds_dm_squid dm inner join ds_squid s on s.id=dm.id  where s.id=?",predictSquidId);
//			Integer isVersion = (Integer) dm_squid.get("VERSIONING");
			Integer secVersion = tf.getModel_version();
			Integer secSFID = (Integer) dm_squid.get("SQUID_FLOW_ID");
			String sql = "select model from "+HbaseUtil.genTrainModelTableName(this.ctx.getRepositoryId(), secSFID, predictSquidId);
			// 判断是否指定key
			boolean isKey = false;
			if(ttf.getInKeyList().size()==1) {
				isKey = false;
			} else if(ttf.getInKeyList().size()==2) {
				isKey = true;
				sql += " where \"KEY\" = ? ";
			}
			if(secVersion== -1){
                sql+=" order by version desc limit 1";
            }else{
				if(isKey) {
					sql += " and version =" + secVersion;
				} else {
					sql += " where version =" + secVersion;
				}
            }
            // 模型的数据查询迁移到运行时，
//			Map<String,Object> data = ConstantUtil.getHbaseJdbc().queryForMap(sql);
			infoMap.put(TTransformationInfoType.PREDICT_MODEL_TYPE.dbValue, SquidTypeEnum.parse((Integer) dm_squid.get("SQUID_TYPE_ID")));
            // 先放SQL
			infoMap.put(TTransformationInfoType.PREDIC_DM_MODEL.dbValue,sql);
			// 如果预测使用了其它squid的产生的模型并且该squid与依赖的squid在同一squid，必须设置依赖.
			if(secSFID==this.ctx.squidFlow.getId()){
				TSquid out = this.ctx.getSquidOut(predictSquidId);
				List<TSquid> depends = tsquid.getDependenceSquids();
				if(depends==null){
					depends= new JList<TSquid>();
					tsquid.setDependenceSquids(depends);
				}
				depends.add(out);
			}
		}
		if (isLastTrsOfSquid(tf)) { // column 指定notnull
			int colId = tf.getColumn_id();
			DataSquid ss = (DataSquid) this.currentSquid;
			for (Column col : ss.getColumns()) {
				if (col.getId() == colId) {
					boolean bool = col.isNullable();
					infoMap.put(TTransformationInfoType.IS_NULLABLE.dbValue, bool);
					infoMap.put(TTransformationInfoType.STRING_MAX_LENGTH.dbValue, col.getLength());
					infoMap.put(TTransformationInfoType.NUMERIC_PRECISION.dbValue, col.getPrecision());
					infoMap.put(TTransformationInfoType.NUMERIC_SCALE.dbValue, col.getScale());
					// 放置的系统类型，主要用于对于smallInt, bigInt,tinyInt等进行数据大小验证
					if(col.getAggregation_type()<=0) {
						infoMap.put(TTransformationInfoType.VIRT_TRANS_OUT_DATA_TYPE.dbValue, col.getData_type());
					} else {
						// 标记这个transformation为 参与聚合的列，不需要对数据进行校验
						if(!col.isIs_groupby()) {
							infoMap.put(TTransformationInfoType.AGGREGATION_COLUMN.dbValue, 1);
						}
					}
					break;
				}
			}
		}
		ttf.setInfoMap(infoMap);

		action.settTransformation(ttf);

		action.setRmKeys(getDropedTrsKeys(tf));

		trsCache.put(tf.getId(), ttf);

		return action;
	}*/


	/**
	 * 取本squid里面的column
	 * @param colId colid.
	 * @return
	 */
	protected Column getColumnById(int colId){
		DataSquid ss = (DataSquid) this.currentSquid;
		for (Column col : ss.getColumns()) {
			if (col.getId() == colId) {
				return col;
			}
		}
		return null;
	}
	protected void prepareLinks() {
		DataSquid ss = (DataSquid) this.currentSquid;
		List<Transformation> trans = ss.getTransformations();
		this.preparedLinks.addAll(ss.getTransformationLinks());
		List<Transformation> begins = this.getBeginTrs();
		if(begins.size()==0) return ;
		int firstTrsId = begins.get(0).getId();
		for (Transformation tf : trans) {
			prepareVariableLinks(tf);
			prepareConstantTrs(tf,firstTrsId);
		}
		
	}

	/**
	 * 判断transformation 是否是常量。有显示常量和隐式常量（如随机数，当前日期）。
	 * @param trs
	 * @return
	 */
	private boolean isConstantTrs(Transformation trs){
		TransformationTypeEnum typeEnum = TransformationTypeEnum.parse(trs.getTranstype());
		switch(typeEnum){
		case CONSTANT:
		case RANDOM:
		case SYSTEMDATETIME:
		case PI:
		case JOBID:
		case PROJECTID:
		case PROJECTNAME:
		case SQUIDFLOWID:
		case SQUIDFLOWNAME:
		case TASKID:
			return true;
		default:
			return false;
		}
	}
	/**
	 * 取TRS的filter。
	 * 
	 * @param tf
	 * @return
	 */
	protected List<TFilterExpression> getFilterList(Transformation tf, Squid squid) {
		List<TFilterExpression> filterLists = new ArrayList<>();
		TransformationTypeEnum tfType = TransformationTypeEnum.parse(tf.getTranstype());
		if (tf.getInputs() != null && tf.getInputs().size() > 0) {
			for (TransformationInputs input : tf.getInputs()) {
				if (tfType.equals(TransformationTypeEnum.CHOICE)) { // 如果是choice
//					filterLists.add(translateTrsFilter(input.getIn_condition(), squid));
					filterLists.add(TranslateUtil.translateTransformationFilter(input.getIn_condition(), (DataSquid)squid, this.trsCache, ctx.getVariable()));
				}
			}
		}
		return filterLists;
	}

	/**
	 * 判断一个transformation是否是最后一个虚拟转换。
	 * 
	 * @param tf
	 * @return
	 */
	protected boolean isLastTrsOfSquid(Transformation tf) {
		DataSquid ss = (DataSquid) this.currentSquid;
		for (Column col : ss.getColumns()) {
			if (col.getId() == tf.getColumn_id()) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 判断一个transformation是否是第一个虚拟转换。
	 * 
	 * @param tf
	 * @return
	 */
	protected boolean isFirstTrsOfSquid(Transformation tf) {
		DataSquid ss = (DataSquid) this.currentSquid;
		for (ReferenceColumn col : ss.getSourceColumns()) {
			if (col.getColumn_id() == tf.getColumn_id()) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 取Transformation的inkeyList.
	 * 
	 * @param tf
	 * @return
	 */
	protected List<Integer> getInkeyList(Transformation tf) {
		List<Integer> inkeys = JList.create();
		if (isFirstTrsOfSquid(tf)) {
			inkeys.add(tf.getColumn_id()); // 如果是第一个transformation,inkey是refcol的ID
		} else {
			if(TransformationTypeEnum.VIRTUAL.value()==tf.getTranstype() && tf.getInputs()==null){
				throw new RuntimeException("column id ["+tf.getColumn_id()+"]对应的transformation inputs 为空");
			}
			for (TransformationInputs input : tf.getInputs()) {
				Integer trsId = input.getSource_transform_id();
				if (trsId == null || trsId.equals(0)) {
					throw new RuntimeException("transformationInput[" + input.getId() + "]的source_transform_id为空,tran_id[" + tf.getId() + "]");
				}
				Integer idx = input.getSource_tran_output_index();
				TTransformation ttf = this.trsCache.get(trsId);
				inkeys.add(ttf.getOutKeyList().get(idx)); // 按照inputs指定上游的Trans。
			}
		}
		return inkeys;
	}

	/**
	 * 生成Transformation的outKeyList.
	 * 默认生成一个outKey,无论有没有线连到它。
	 * @param tf
	 * @return
	 */
	protected List<Integer> genOutKeyList(Transformation tf) {
		int outputs = tf.getOutput_number();
		outputs = outputs > 0 ? outputs : 1;
		List<Integer> outKeys = JList.create();
		if (isLastTrsOfSquid(tf)) {
			outKeys.add(tf.getColumn_id());
		} else if (isFirstTrsOfSquid(tf)) {
			outKeys.add(-tf.getColumn_id());
		} else {
			for (int i = 0; i < outputs; i++) {
				outKeys.add(this.genOutKey());
			}
		}
		return outKeys;
	}

	/**
	 * 取transformation的outKey.
	 * 
	 * @param tf
	 * @return
	 */
	protected List<Integer> getOutKeyListByTrs(Transformation tf) {
		return this.trsCache.get(tf.getId()).getOutKeyList();
	}

	/**
	 * 依据表达式创建虚拟link.
	 * 
	 * @param tf
	 * @return
	 */
	protected void prepareConstantTrs(Transformation tf,int firstTrsId) {
		if(isConstantTrs(tf)){
			TransformationLink link = new TransformationLink();
			link.setFrom_transformation_id(firstTrsId);
			link.setTo_transformation_id(tf.getId());
			preparedLinks.add(link);
		}
	}
	/**
	 * 依据表达式创建虚拟link.
	 * 
	 * @param tf
	 * @return
	 */
	protected void prepareVariableLinks(Transformation tf) {
		ArrayList<String> condList = new ArrayList<>();
		if(tf.getTranstype()== TransformationTypeEnum.CHOICE.value()){
			for(TransformationInputs input :tf.getInputs()){
				condList.add(input.getIn_condition());
			}
		}
		condList.add(tf.getTran_condition());
		for(String cond: condList){
			if (!ValidateUtils.isEmpty(cond)) {
				Matcher nameMatcher = StringUtils.match(cond, "#([^>=<\\s]*)");		// transformation
	
				while (nameMatcher.find()) {
					String tName = nameMatcher.group(1).trim();
					TransformationLink link = new TransformationLink();
					link.setFrom_transformation_id(getTranByName(tName).getId());
					link.setTo_transformation_id(tf.getId());
					preparedLinks.add(link);
				}
				nameMatcher = StringUtils.match(cond, "\\.([^>=<\\s]*)");		// referenceColumn
				while (nameMatcher.find()) {
					String tName = nameMatcher.group(1).trim();
					if(ValidateUtils.isNumeric(tName)) continue;		// 数字的1.0会与squid.col 冲突。
					TransformationLink link = new TransformationLink();
					link.setFrom_transformation_id(getTranByColumnName(tName).getId());
					link.setTo_transformation_id(tf.getId());
					preparedLinks.add(link);
				}
			}
		}

	}

	/**
	 * 取指定trs的输入Trans
	 * 
	 * @param tf
	 * @return
	 */
	protected List<Transformation> getInTrs(Transformation tf) {
		List<Transformation> ret = new ArrayList<>();
		for (TransformationLink link : preparedLinks) {
			if (link.getTo_transformation_id() == tf.getId()) {
				Transformation outTrs = this.getTranById(link.getFrom_transformation_id());
				ret.add(outTrs);
			}
		}
		return ret;
	}

	/**
	 * 取指定trs的输出Trans
	 * 
	 * @param tf
	 * @return
	 */
	protected List<Transformation> getOutTrs(Transformation tf) {
		List<Transformation> ret = new ArrayList<>();
		for (TransformationLink link : preparedLinks) {
			if (link.getFrom_transformation_id() == tf.getId()) {
				Transformation outTrs = this.getTranById(link.getTo_transformation_id());
				ret.add(outTrs);
			}
		}
		return ret;
	}

	/**
	 * 抛弃transformation.并且返回可用的抛弃值。
	 * 
	 * @param tf
	 * @return
	 */
	protected HashSet<Integer> getDropedTrsKeys(Transformation tf) {
		List<Transformation> ins = this.getInTrs(tf);
		HashSet<Integer> ret = new HashSet<>();
		if (isFirstTrsOfSquid(tf)) { // 如果是第一个，那么移除column key.
			ret.add(tf.getColumn_id());
		} else {
			for (Transformation in : ins) {

				Integer outSize = this.getOutTrs(in).size();

				Integer dropCount = squidDropMapper.get(in.getId());

				dropCount = dropCount == null ? 1 : dropCount + 1; // 每次抛弃后加1次抛弃值。

				if (dropCount >= outSize) { // 抛弃值》= 输出值 时，此transformation 寿命已到。
					List<Integer> outKeys = this.getOutKeyListByTrs(in);
					if (outKeys != null) {
						for (Integer x : outKeys) {
							if (x > 0) {
								ret.add(x);
							}
						}
					}
				}

				squidDropMapper.put(in.getId(), dropCount);

			}
		}
		return ret;
	}

	/**
	 * 按名字找到transformation
	 * 
	 * @param name
	 * @return
	 */
	protected Transformation getTranByName(String name) {
		DataSquid ss = (DataSquid) this.currentSquid;
		for (Transformation tf : ss.getTransformations()) {
			if (name.equals(tf.getName())) {
				return tf;
			}
		}
		return null;
	}
	/**
	 * 按照olumnId匹配 virtual Transformation.
	 * @param colId referenceColumn id或者 column id.
	 * @return
	 */
	protected Transformation getTranByColumnId(Integer colId) {
		DataSquid ss = (DataSquid) this.currentSquid;
		for (Transformation tf : ss.getTransformations()) {
			if (colId.equals(tf.getColumn_id())) {
				return tf;
			}
		}
		return null;
	}
	/**
	 * 按照olumnName匹配 virtual Transformation.
	 * @param colName referenceColumn id或者 column id.
	 * @return
	 */
	protected Transformation getTranByColumnName(String colName) {
		DataSquid ss = (DataSquid) this.currentSquid;
		
		for (ReferenceColumn rc : ss.getSourceColumns()) {
			if (colName.equals(rc.getName())) {
				return getTranByColumnId(rc.getColumn_id());
			}
		}
		return null;
	}
	
	protected Transformation getTranById(int id) {
		DataSquid ss = (DataSquid) this.currentSquid;
		for (Transformation tf : ss.getTransformations()) {
			if (tf.getId() == id) {
				return tf;
			}
		}
		return null;
	}

}
