package com.eurlanda.datashire.engine.translation;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.eurlanda.datashire.adapter.HyperSQLManager;
import com.eurlanda.datashire.engine.entity.*;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.engine.util.DebugUtils;
import com.eurlanda.datashire.engine.util.cool.JList;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.dest.*;
import com.eurlanda.datashire.server.model.DataCatchSquid;
import com.eurlanda.datashire.server.model.PivotSquid;
import com.eurlanda.datashire.server.model.SamplingSquid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.*;

/**
 * 编译器上下文
 * 
 * @author Gene
 * 
 */
public class BuilderContext {
    /**
     * 给每个squid定义 builder.每个builder可以build出不同的TSquid.
     */
    private static Map<Class, Class> builders = new HashMap<Class, Class>();
    /**
     * squidName->Squid
     */
    public Map<String, Squid> name2squidMap = new HashMap<>();
    public Integer repositoryId;
    //	private List<Integer> excludeKeys = new ArrayList<>();
    public String taskId;
    // debug 运行到
	protected List<Integer> debugDestinations = new ArrayList<Integer>();
    protected List<Integer> debugBreakPoints = new ArrayList<Integer>();
    protected List<Integer> debugDataViewer = new ArrayList<Integer>();
    protected List<Integer> squids = new ArrayList<Integer>();
    // column 与 map中key的映射关系 , transformation -> new Id,Column -> newId
	// squidFlow中每一个column,transformation 对应到 TsquidFlow 中的 Id see #generate()
	protected Map<String, Integer> columnKeyMap = new HashMap<>();
    /**
	 * squidID->Squid
	 */
	protected Map<Integer, Squid> id2squidMap = new HashMap<>();
    // squidFlow 中所有的SquidLink;
	protected List<SquidLink> squidLinks;
    protected List<TSquid> allBuildedSquids = new ArrayList<>();
    protected SquidFlow squidFlow;
    //	protected int curOutKey = 0;
    protected int jobId;
    protected String jobName;
    /**
     * 已经翻译过的Squid
     */
    protected Map<Squid, List<TSquid>> buildedMapper = new HashMap<>();
	/**
	 * 一个squid下游多个samplingSquid
	 */
	protected Map<String,TSamplingSquid> sampSquidMap = new HashMap<>();
    protected Project project;

    // variable
	private Map<String, DSVariable> variable;
    private IdKeyGenerator idKeyGenerator = new IdKeyGenerator();
	private Logger logger = LoggerFactory.getLogger(BuilderContext.class);
	private boolean debugMode = false;
	static {

		builders.put(ExtractSquid.class, SqlExctractBuilder.class);
		builders.put(StageSquid.class, StageBuilder.class);
		builders.put(ExceptionSquid.class, ExceptionBuilder.class);
		builders.put(ReportSquid.class, ReportBuilder.class);
		builders.put(WebExtractSquid.class, WebExtractBuilder.class);
		builders.put(WeiBoExtractSquid.class, WeiboExtractBuilder.class);
		builders.put(DocExtractSquid.class, DocExtractBuilder.class);
		builders.put(WebLogExtractSquid.class, LogExtractBuilder.class);
		builders.put(XmlExtractSquid.class, XmlExtractBuilder.class);
		builders.put(DataMiningSquid.class, DataMingSquidBuilder.class);
		builders.put(GISMapSquid.class, ReportBuilder.class);
		builders.put(MongodbExtractSquid.class, MongodbExctractBuilder.class);
		builders.put(DestESSquid.class, DestESSquidBuilder.class);
		builders.put(HBaseExtractSquid.class, HBaseSquidBuilder.class);
        builders.put(DestHDFSSquid.class, DestHdfsSquidBuilder.class);
        builders.put(DestImpalaSquid.class, DestImpalaSquidBuilder.class);
        builders.put(GroupTaggingSquid.class, GroupTaggingSquidBuilder.class);
        builders.put(SystemHiveExtractSquid.class, HiveSquidBuilder.class);
        builders.put(CassandraExtractSquid.class, CassandraSquidBuilder.class);
        builders.put(UserDefinedSquid.class, UserDefinedSquidBuilder.class);
        builders.put(DestHiveSquid.class, DestSystemHiveSquidBuilder.class);
		builders.put(StatisticsSquid.class, StatisticsSquidBuilder.class);
		builders.put(DestCassandraSquid.class, DestCassandraSquidBuilder.class);
		builders.put(DataCatchSquid.class, DataCatchSquidBuilder.class);
		builders.put(DataCatchSquid.class, CoefficientSquidBuilder.class);
		builders.put(SamplingSquid.class,SampingSquidBuilder.class);
	    builders.put(PivotSquid.class,PivotSquidBuilder.class);
	}

	/**
	 * 转换SquidFlow为spark引擎可执行的 TSquidFlow.
	 * 
	 * @param sf
	 * @return
	 */
	public BuilderContext(Integer repositoryId,SquidFlow sf) {
		this.squidFlow = sf;
		squidLinks = sf.getSquidLinkList();
		// 排除一些key
		for (Squid s : sf.getSquidList()) {
			if (s instanceof DataSquid) {
				for (Column col : ((DataSquid) s).getColumns()) {
                    idKeyGenerator.addExcludeKey(col.getId());
				}
			}
		}
		DataSource ds = ConstantUtil.getSysDataSource();
		String sql="select * from ds_project where id="+sf.getProject_id();
		try {
			this.project= HyperSQLManager.query2List(ds.getConnection(), true, sql, null, Project.class).get(0);
			this.repositoryId= project.getRepository_id();
		} catch (SQLException e) {
			throw new RuntimeException("获取project["+sf.getProject_id()+"]失败！");
		}
	}

	/**
	 * 转换SquidFlow为带debug的，spark引擎可执行的 TSquidFlow.
	 * 
	 * @param squid
	 * @param debugString
	 * @return
	 */
	public BuilderContext(Integer repositoryId,SquidFlow squid, String debugString) {
		this(repositoryId,squid);
		Map<String, JSONArray> obj = (Map<String, JSONArray>) JSON.parse(debugString);
		logger.info("处理debug字符串：" + debugString);
		JSONArray breakpoints = (JSONArray) obj.get("breakPoints");
		if (breakpoints != null) {
			for (Object x : breakpoints) {
				if (x instanceof java.lang.Number) {
					this.debugBreakPoints.add(((java.lang.Number) x).intValue());
				}
			}
		}
		JSONArray dataViewers = (JSONArray) obj.get("dataViewers");
		if (dataViewers != null) {
			for (Object x : dataViewers) {
				if (x instanceof java.lang.Number) {
					this.debugDataViewer.add(((java.lang.Number) x).intValue());
				}
			}
		}
		JSONArray destinations = (JSONArray) obj.get("destinations");
		if (destinations != null) {
			for (Object x : destinations) {
				if (x instanceof java.lang.Number) {
					this.debugDestinations.add(((java.lang.Number) x).intValue());
				}
			}
		}
		this.debugMode = true;
	}

	/**
	 * 编译squidFlow.
	 * 
	 * @return
	 */
	public TSquidFlow build() {
		TSquidFlow tsf = new TSquidFlow();
		tsf.setDebugModel(this.debugMode);
		tsf.setName(squidFlow.getName());
		tsf.setProjectId(squidFlow.getProject_id());
		tsf.setRepositoryId(this.getRepositoryId());
		tsf.setJobName(this.jobName);
		// 剪枝
		List<Squid> cuttedSquis = new SquidCutterBuilder(this).cut();

		for (Squid curSquid : cuttedSquis) {
			this.build(curSquid);
		}
		tsf.setSquidList(allBuildedSquids);
		logger.debug("squid flow 翻译完成，共：{} 个", allBuildedSquids.size());
		DebugUtils.printSquidFlow(tsf);

		return tsf;
	}

	/**
	 * build一个 squid.
	 * 
	 * @param squid
	 * @return
	 */
	private void build(Squid squid) {
		// 判断其前面的Squid是否已经翻译，如果没有翻译先翻译
		List<Squid> preSquids = getPrevSquids(squid);
		for (Squid sq : preSquids) {
			if (!this.buildedMapper.containsKey(sq)) {
				 this.build(sq);
			}
		}
		if(this.buildedMapper.containsKey(squid)){
			return ;
		}
		List<TSquid> currentBuildedSquids = new ArrayList<>();
		try {
			// 更新缓存
			id2squidMap.put(squid.getId(), squid);
			name2squidMap.put(squid.getName(), squid);

			Class builderClasses = builders.get(squid.getClass());
			if (builderClasses != null) {
				// 调用编译器编译。
				TSquidBuilder builder = this.getBuilder(builderClasses, this, squid);
				if (builder != null) {
					List<TSquid> buildedSquids = builder.translate(squid); // builder
					if (buildedSquids != null) {
						for (TSquid x : buildedSquids) {
							if (x != null) {
                                x.setName(squid.getName());
                                /**
                                 * 当为samplingsquid的时候，规则为:
                                 * 将同一个squid的下游所有的samplingsquid放到一个map里面 key:preId_curr_id,value SamplingSquid
                                 * 把samplingsquid当做一个samplingsquid处理
                                 */
								if(x.getType()==TSquidType.SAMPLINGSQUID){
									String key = null;
									if(preSquids!=null && preSquids.size()>0){
										key = preSquids.get(0).getName()+"_"+preSquids.get(0).getId();
									}
									if(sampSquidMap.containsKey(key)){
										sampSquidMap.get(key).getSquidList().add((TSamplingSquid) x);
									} else {
										sampSquidMap.put(key,(TSamplingSquid) x);
									}
								}
								currentBuildedSquids.add(x); // 添加编译后的squids.
							}
						}
					}
				}
			}
			if (this.debugMode && currentBuildedSquids.size() > 0) {
				// build debug.
				TDebugSquid ds = new TDebugSquid();
				ds.setBreakPoint(false);
				ds.setDataView(false);
				ds.setSquidId(squid.getId());
                ds.setName(squid.getName());

				// DataSquid dataSquid = (DataSquid)squid;
				for (TSquid tsquid : currentBuildedSquids) {
					if (tsquid instanceof TDataFallSquid || tsquid instanceof TMongoDataFallSquid) {
						ds.setDependenceSquids(new JList<TSquid>(tsquid));
					} else {
						ds.setPreviousSquid(tsquid);
					}
				}
				if (this.debugBreakPoints.contains(squid.getId())) {
					ds.setBreakPoint(true);
				}
				if (this.debugDataViewer.contains(squid.getId())) {
					ds.setDataView(true);
				}
				currentBuildedSquids.add(ds);
			}

			buildedMapper.put(squid, currentBuildedSquids);
		} catch (Exception e) {
			logger.error("翻译{}出错: squidName:{} squidId:{} repositoryId:{} squidFlowId:{}",
                    new Object[] {   squid.getClass().getSimpleName(),squid.getName(),
                            squid.getId(), this.repositoryId, this.squidFlow.getId() });
			e.printStackTrace();
			throw e;
		}
		allBuildedSquids.addAll(currentBuildedSquids);
	}

	/**
	 * new 一个builder
	 * 
	 * @param builderClass
	 * @param args
	 * @return
	 */
	private TSquidBuilder getBuilder(Class<?> builderClass, Object... args) {
		try {
			Constructor<?> conts = builderClass.getConstructors()[0];
			return (TSquidBuilder) conts.newInstance(args);
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 取所有链接到本squid的前置squid.
	 * 
	 * @return
	 */
	public List<Squid> getPrevSquids(Squid squid) {
		List<Squid> squidList = new ArrayList<>();
		for (SquidLink sl : squidLinks) {
			if (sl.getTo_squid_id() == squid.getId()) {
				squidList.add(getSquidById(sl.getFrom_squid_id()));
			}
		}
		return squidList;
	}

	/**
	 * 取所有链接到本squid的前置squid.
	 * 
	 * @return
	 */
	public <T> T getPrevSquid(Squid squid,Class<T> cls) {
		for (SquidLink sl : squidLinks) {
			if (sl.getTo_squid_id() == squid.getId()) {
				Squid sq = getSquidById(sl.getFrom_squid_id());
				if(sq!=null){
					if(sq.getClass().equals(cls)){
						return (T) sq;
					}else{
						return getPrevSquid(sq, cls);
					}
				}
				break;
			}
		}
		return null;
	}

	/**
	 * 获取链接到本squid上面的所有的前置的squid
	 * @param squidId
	 * @return
	 */
	public List<Squid> getPrevSquidById(int squidId){
		List<Squid> squidList = new ArrayList<>();
		for (SquidLink sl : squidLinks) {
			if (sl.getTo_squid_id() == squidId) {
				squidList.add(getSquidById(sl.getFrom_squid_id()));
			}
		}
		return squidList;
	}
	public List<Squid> getNextSquids(Squid squid) {
		List<Squid> squidList = new ArrayList<>();
		for (SquidLink sl : squidLinks) {
			if (sl.getFrom_squid_id() == squid.getId()) {
				squidList.add(getSquidById(sl.getTo_squid_id()));
			}
		}
		return squidList;
	}
	/**
	 * 取squid的输出squid.
	 * 
	 * @param prevSquid
	 * @return
	 */
	protected TSquid getSquidOut(Squid prevSquid) {
		return getSquidOut(prevSquid.getId());
	}

	/**
	 * 取squid的输出squid. TExceptionSquid例外
	 * 
	 * @param prevSquidId
	 * @return
	 */
	public TSquid getSquidOut(Integer prevSquidId) {
		List<TSquid> tSquids = buildedMapper.get(id2squidMap.get(prevSquidId));
		if(tSquids==null){
			Squid squid = this.getSquidById(prevSquidId);
			this.build(squid);
		}
		tSquids = buildedMapper.get(id2squidMap.get(prevSquidId));
		if (tSquids.size() > 0) {
			for (int i = tSquids.size() - 1; i >= 0; i--) {
				TSquid ts = tSquids.get(i);
				if (ts instanceof TExceptionSquid
						|| ts instanceof TDataFallSquid
					) {
					continue;
				}
				return ts;
			}
		}
		return null;
	}

	/**
	 * 取squid的落地squid.
	 * 
	 * @return
	 */
	public TDataFallSquid getSquidDataFallOut(Squid prevSquid) {
		return getSquidDataFallOut(prevSquid.getId());
	}

	/**
	 * 取squid的落地squid.
	 * 
	 * @param prevSquidId
	 * @return
	 */
	protected TDataFallSquid getSquidDataFallOut(Integer prevSquidId) {
		List<TSquid> tSquids = buildedMapper.get(id2squidMap.get(prevSquidId));
		// 只有三种情况，直接遍历 TODO 以后重构
		for (TSquid ts : tSquids) {
			if (ts.getType() == TSquidType.DATA_FALL_SQUID)
				return (TDataFallSquid) ts;
		}
		return null;
	}

	/**
	 * 取squid的翻译之后的指定类型的TSquid.
	 * 
	 * @param squid
	 * @param cls
	 *            指定的类型。
	 * @return
	 */
	public <T> T getSquidOut(Squid squid, Class<T> cls) {
		return getSquidOut(squid.getId(), cls);
	}

	/**
	 * 取squid的翻译之后的指定类型的TSquid.
	 * 
	 * @param squidId
	 * @param cls
	 *            指定的类型。
	 * @return
	 */
	public <T> T getSquidOut(Integer squidId, Class<T> cls) {
		List<TSquid> tSquids = buildedMapper.get(id2squidMap.get(squidId));
		// 只有三种情况，直接遍历 TODO 以后重构
		for (TSquid ts : tSquids) {
			if (ts.getClass().equals(cls))
				return (T) ts;
		}
		return null;
	}

	/**
	 * 根据squid id 查找 squid
	 * 
	 * @param id
	 * @return
	 */
	public Squid getSquidById(int id) {
		Squid squid = id2squidMap.get(id);
		if (squid == null) {
			for (Squid x : this.squidFlow.getSquidList()) {
				if (x.getId() == id) {
					id2squidMap.put(id, x);
					return x;
				}
			}
		}
		return squid;
	}

	public Squid getSquidByName(String name) {

		Squid squid = name2squidMap.get(name);
		if (squid == null) {
			for (Squid x : this.squidFlow.getSquidList()) {
				if (x.getName().equals(name)) {
					name2squidMap.put(name, x);
					return x;
				}
			}
		}
		return squid;
	}

	/**
	 * 取得一个输出key,key递增的数字。
	 * 
	 * @return 输出的key.
	 */
	public Integer genOutKey() {
        return idKeyGenerator.genKey();
	}

	public Integer getRepositoryId() {
		return repositoryId;
	}

	public void setRepositoryId(Integer repositoryId) {
		this.repositoryId = repositoryId;
	}

	public String getTaskId() {
		return taskId;
	}

	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public boolean isDebugMode() {
		return debugMode;
	}

	public void setDebugMode(boolean debugMode) {
		this.debugMode = debugMode;
	}

	public int getJobId() {
		return jobId;
	}

	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

	public Map<String, DSVariable> getVariable() {
		return variable;
	}

    public IdKeyGenerator getIdKeyGenerator() {
        return idKeyGenerator;
    }

    public Map<Squid, List<TSquid>> getBuildedMapper() {
        return buildedMapper;
    }

    public void setVariable(Map<String, DSVariable> variable) {
		this.variable = variable;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public Map<String, TSamplingSquid> getSampSquidMap() {
		return sampSquidMap;
	}

	public void setSampSquidMap(Map<String, TSamplingSquid> sampSquidMap) {
		this.sampSquidMap = sampSquidMap;
	}
}
