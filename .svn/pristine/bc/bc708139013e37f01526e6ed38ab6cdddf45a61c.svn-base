package com.eurlanda.datashire.engine.translation;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.eurlanda.datashire.common.webService.SquidEdit;
import com.eurlanda.datashire.common.webService.StartController;
import com.eurlanda.datashire.common.webService.StartTypeEnum;
import com.eurlanda.datashire.engine.dao.SquidFlowDao;
import com.eurlanda.datashire.engine.entity.TSquidFlow;
import com.eurlanda.datashire.engine.schedule.ScheduleService;
import com.eurlanda.datashire.engine.util.ConfigurationUtil;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.entity.DSVariable;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.SquidFlow;
import com.eurlanda.datashire.server.model.ScheduleJob;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.reflect.Field;
import java.util.*;

/**
 * 转换器。
 *
 * @author Gene
 *
 */
public class Translator {

	private static Log log = LogFactory.getLog(Translator.class);

	/**
	 * 将SquidFlow编译为Spark可运行的TSquidFlow.
	 *
	 * @param taskId
	 *            任务ID
	 * @param sf
	 *            SF定义
	 * @param repositoryId
	 *            仓库ID
	 * @param debug
	 *            debug JSON字符串，格式如下<br/>
	 *            {"breakPoints":[12],"dataViewers":[11,12],"destinations":[12]}
	 *            ";<br/>
	 *            breakPoints:断点列表，值为断点所在squid的squidID<br/>
	 *            dataViewers:数据查看器列表，值为查看器所在squid的squidID<br/>
	 *            destinations:运行目标，值为运行目标所在squid的squidID<br/>
	 * @return
	 */
	public static TSquidFlow build(String taskId, SquidFlow sf, Integer repositoryId, List<DSVariable> variableList, String debug) {
		BuilderContext ctx = new BuilderContext(repositoryId,sf, debug);
		ctx.setTaskId(taskId);
		// 获取变量 project、squidflow 级别的变量key 为变量名，squid级别的变量key为 squidId + _ + 变量名
		Map<String, DSVariable> vs = new HashMap<>();
		for(DSVariable var : variableList) {
			if(var.getSquid_id() == 0) {
				vs.put(var.getVariable_name(), var);
			} else {
				vs.put(var.getSquid_id() + "_" + var.getVariable_name(), var);
			}
		}
		ctx.setVariable(vs);
		return ctx.build();
	}

	public static TSquidFlow build(String taskId, SquidFlow sf, Integer repositoryId, List<DSVariable> variableList, Integer jobId) {
		BuilderContext ctx = new BuilderContext(repositoryId,sf);
		ctx.setTaskId(taskId);
//		ctx.setRepositoryId(repositoryId);
		ctx.setJobId(jobId);
		//获取调度的名字
		if(jobId!=null){
			ScheduleJob job = ScheduleService.getInstance().getScheduleJobById(jobId);
			if(job!=null){
				ctx.setJobName(job.getName());
			}
		}
		// 获取变量 project、squidflow 级别的变量key 为变量名，squid级别的变量key为 squidId + _ + 变量名
		Map<String, DSVariable> vs = new HashMap<>();
		for(DSVariable var : variableList) {
			if(var.getSquid_id() == 0) {
				vs.put(var.getVariable_name(), var);
			} else {
				vs.put(var.getSquid_id() + "_" + var.getVariable_name(), var);
			}
		}
		ctx.setVariable(vs);
		return ctx.build();
	}

	public static TSquidFlow buildProxy(String taskId, Integer jobId, Integer repositoryId, Integer squidFlowId, String debug) {
		try {
			return buildProxy(taskId, jobId, repositoryId, squidFlowId, debug, null, null);
		}catch (IllegalAccessException iae){
			iae.printStackTrace();
		}
		return null;
	}
	public static TSquidFlow buildProxy(String taskId, Integer jobId, Integer repositoryId, Integer squidFlowId, String debug,List<SquidEdit> squidEdits,Map<String,String> variableEdits) throws IllegalAccessException {
		TSquidFlow tSquidFlow;
		SquidFlow sf = getSquidFlow(repositoryId, squidFlowId,squidEdits);
        if(sf == null) {
            log.error("squidflow无法找到,squidflowId:" + squidFlowId);
            return null;
        }
		List<DSVariable> list = getVariableList(squidFlowId, sf.getProject_id(),variableEdits);

		if (StringUtils.isBlank(debug)) {
			tSquidFlow = Translator.build(taskId, sf, repositoryId, list, jobId);
		} else {
			Map<String, JSONArray> obj = (Map<String, JSONArray>) JSON.parse(debug);
			if (MapUtils.isEmpty(obj)) {
				tSquidFlow = build(taskId, sf, repositoryId, list, jobId);
			} else {
				tSquidFlow = build(taskId, sf, repositoryId,list, debug);
			}
		}

		// TODO: 2017/8/16  根据squidFlowId查找出当前允许的最大的并行数,以及单个任务运行的Task数量,用户唯一的标志
		//连接datashire_cloud数据库，获取到套餐的相关信息
		int maxRunningTask=0;
		int maxparallel=0;
		int hdfsSpaceLimit = 0;
		int userId = 0;
		/*if(sf.getDataShireFieldType()>0){
			//如果不是本地场
			//查询出repository信息
			Map<String,Object> repositoryMap = ConstantUtil.getJdbcTemplate().queryForMap("select packageId from ds_sys_repository where id="+repositoryId);
			int packId = (Integer) repositoryMap.get("packageId");
			List<Map<String,Object>> mapList = ConstantUtil.getWebCloudJdbcTemplate().queryForList("select * from packages where id="+packId);
			if(mapList!=null && mapList.size()>0){
				Map<String,Object> map = mapList.get(0);
				maxRunningTask =(Integer) map.get("maxtask");
				maxparallel = (Integer) map.get("maxparallel");
				userId = repositoryId;
			}
		} else {
			//如果是本地数据量，读取本地配置
			maxRunningTask = Integer.parseInt(ConfigurationUtil.getProperty("spark.datashire.scheduler.maxrunningtask"));
			maxparallel = ConfigurationUtil.getParallelNum();
			//如果是本地场，那么userId为repositoryId
			userId = repositoryId;
		}*/
		Map<String,Object> repositoryMap = ConstantUtil.getJdbcTemplate().queryForMap("select packageId from ds_sys_repository where id="+repositoryId);
		if(repositoryMap.get("packageId")==null){
			//本地场
			maxRunningTask = Integer.parseInt(ConfigurationUtil.getProperty("spark.datashire.scheduler.maxrunningtask"));
			maxparallel = ConfigurationUtil.getParallelNum();
			//如果是本地场，那么userId为repositoryId
			userId = repositoryId;
		} else {
			//云端数猎场
			int packId = (Integer) repositoryMap.get("packageId");
			List<Map<String,Object>> mapList = ConstantUtil.getWebCloudJdbcTemplate().queryForList("select * from packages where id="+packId);
			if(mapList!=null && mapList.size()>0){
				Map<String,Object> map = mapList.get(0);
				maxRunningTask =(Integer) map.get("maxtask");
				maxparallel = (Integer) map.get("maxparallel");
				hdfsSpaceLimit = (Integer) map.get("fileCapacity");
				userId = repositoryId;
			}
		}
		tSquidFlow.setMaxRunningTask(maxRunningTask);
		tSquidFlow.setUserId(userId);
		tSquidFlow.setMaxParallelNum(maxparallel);
		tSquidFlow.setTaskId(taskId);
		tSquidFlow.setRepositoryId(repositoryId);
		tSquidFlow.setId(squidFlowId);
		tSquidFlow.setHdfsSpaceLimit(hdfsSpaceLimit);
		tSquidFlow.setJobId(jobId != null ? jobId : -1);
		setTSquidFlowInfo(tSquidFlow);

		return tSquidFlow;
	}


	/**
	 * 按仓库ID,取squidFlow
	 *
	 * @param repositoryId
	 * @param squidFLowId
	 * @return
	 */
	private static SquidFlow getSquidFlow(Integer repositoryId, Integer squidFLowId) {
		try {
			return getSquidFlow(repositoryId, squidFLowId, null);
		}catch (IllegalAccessException iae){
			iae.printStackTrace();
		}
		return null;
	}

	/**
	 * 按仓库ID,取squidFlow
	 * @param repositoryId
	 * @param squidFLowId
	 * @param squidEdits
	 * @return
	 */
	public static SquidFlow getSquidFlow(Integer repositoryId, Integer squidFLowId,List<SquidEdit> squidEdits) throws IllegalAccessException {
		SquidFlowDao dao = ConstantUtil.getSquidFlowDao();
		SquidFlow squidFlow = dao.getSquidFlow(squidFLowId);
		if(squidEdits!=null&&squidFlow!=null&&squidFlow.getSquidList()!=null&&squidFlow.getSquidList().size()>0) {
			for (SquidEdit squidEdit : squidEdits) {
				for (Squid squid:squidFlow.getSquidList()){
					if(squid.getName().equals(squidEdit.getSuiqidName())){
						updateField(squid,squidEdit.getPropertyName(),squidEdit.getValue());
					}
				}
			}
		}
		return squidFlow;
	}

	private static void updateField(Object obj,String fieldName,Object value) throws IllegalAccessException {
		Class<?> cl = obj.getClass();
		for (Field field:getField(cl)){
			StartController sta = field.getAnnotation(StartController.class);
			if (sta != null && sta.valid()== StartTypeEnum.valid) {
				if (fieldName.equals(sta.propertyName())) {
					field.setAccessible(true);
					field.set(obj, value);
				}
			}
		}
	}

	private static List<Field> getField(Class<?> cl){
		List<Field> listField = null;
		if(cl.getSuperclass()==null){
			listField=new ArrayList<Field>();
		}else {
			listField = getField(cl.getSuperclass());
		}
		for(Field field:cl.getDeclaredFields()){
			listField.add(field);
		}
		return listField;
	}

	/**
	 * 获取squidflow 中可能使用的变量
	 * @param squidflowId
	 * @param projectId
	 * @return
	 */
	private static List<DSVariable> getVariableList(Integer squidflowId, Integer projectId) {
		return getVariableList(squidflowId,projectId,null);
	}

	/**
	 * 获取squidflow 中可能使用的变量
	 * @param squidflowId
	 * @param projectId
	 * @param modifys
	 * @return
	 */
	private static List<DSVariable> getVariableList(Integer squidflowId, Integer projectId,Map<String,String> modifys) {
		SquidFlowDao dao = ConstantUtil.getSquidFlowDao();
		List<DSVariable> listDSVariable= dao.getVariableList(squidflowId, projectId);
		if(modifys!=null){
			for(DSVariable dsVariable:listDSVariable){
				if(modifys.containsKey(dsVariable.getVariable_name())) {
					dsVariable.setVariable_value(modifys.get(dsVariable.getVariable_name()));
				}
			}
		}
		return listDSVariable;
	}

	/**
	 * 设置squidflow其他信息
	 * 1.projectName
	 * 2.repositoryName
	 * @param tSquidFlow
	 */
	public static void setTSquidFlowInfo(TSquidFlow tSquidFlow) {
		SquidFlowDao squidFlowDao = ConstantUtil.getSquidFlowDao();
		Map<String, Object> infoMap = squidFlowDao.getSquidFlowInfo(tSquidFlow.getId());
		if(infoMap != null) {
			// 设置 projectName
			tSquidFlow.setProjectName((String)infoMap.get("PROJECTNAME"));
			// 设置 repositoryName
			tSquidFlow.setRepositoryName((String)infoMap.get("REPOSITORYNAME"));
		}
	}

	// String debugString = "{\"dataViewers\":[233]}";
	// SquidFlow squidFlow = getSquidFlow("prod", 14);
	public static void main(String[] args) {

		Integer repositoryId = 3;		// 仓库id

		Integer sfid =198;		//squid flow id

		String taskId = UUID.randomUUID().toString();
		int jobId = 1000;

		SquidFlow squidFlow = getSquidFlow(repositoryId, sfid);
		List<DSVariable> list = getVariableList(sfid, squidFlow.getProject_id());
		TSquidFlow tf = build(taskId, squidFlow, repositoryId, list, jobId);
		System.out.println(tf);
	}
}
