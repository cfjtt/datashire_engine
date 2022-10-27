package com.eurlanda.datashire.common.rpc.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.eurlanda.datashire.common.rpc.IEngineService;
import com.eurlanda.datashire.common.rpc.server.EngineRPCServer;
import com.eurlanda.datashire.engine.dao.ApplicationStatusDao;
import com.eurlanda.datashire.engine.dao.SquidFlowDao;
import com.eurlanda.datashire.engine.entity.TDebugSquid;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.engine.entity.TSquidFlow;
import com.eurlanda.datashire.engine.schedule.ScheduleJobManager;
import com.eurlanda.datashire.engine.schedule.ScheduleService;
import com.eurlanda.datashire.engine.schedule.job.EngineScheduleJob;
import com.eurlanda.datashire.engine.service.EngineService;
import com.eurlanda.datashire.engine.service.SquidFlowLauncher;
import com.eurlanda.datashire.engine.translation.SquidFlowValidator;
import com.eurlanda.datashire.engine.translation.TransformationValidator;
import com.eurlanda.datashire.engine.util.ConfigurationUtil;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.entity.SquidFlow;
import com.eurlanda.datashire.server.model.ScheduleJob;
import org.apache.avro.AvroRemoteException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.quartz.SchedulerException;

import java.util.HashMap;
import java.util.Map;

/**
 * 引擎对外接口实现 Created by zhudebin on 14-4-15.
 */
public class EngineServiceImpl implements IEngineService {

	private static Log log = LogFactory.getLog(EngineServiceImpl.class);
	@Override
	public String launchEngineJob(int squidFlowId, int repositoryId, CharSequence ddvSquids, CharSequence config) throws AvroRemoteException {
		try {
			log.debug("launchEngineJob RPC 接收启动命令：repId:" + repositoryId + ",sfId:" + squidFlowId + ",ddv:" + ddvSquids);
			// {"breakPoints":[12],"dataViewers":[11,12],"destinations":[12]}
			return EngineService.launchJob(squidFlowId, repositoryId, ddvSquids.toString());
		} catch (Exception e) {
            log.error("运行squidflow异常,id:" + squidFlowId, e);
			throw new AvroRemoteException(e.getMessage());
		}
	}

	@Override
	public boolean resumeEngine(CharSequence taskId, int debugSquidId, CharSequence config) throws AvroRemoteException {
		log.debug("resumeEngine RPC 接收启动命令：taskId:" + taskId + ",debugSquidId:" + debugSquidId);
		TSquidFlow squidFlow = EngineRPCServer.getInstance().getSquidFlow(taskId.toString());
		if (squidFlow == null)
//			log.debug("没有找到该squidflow,taksId" + taskId + ",debug :");
			return false;
		for (TSquid s : squidFlow.getSquidList()) {
			if (s.getSquidId() == debugSquidId && s instanceof TDebugSquid) {
				((TDebugSquid) s).release();
				log.debug("继续运行该 squidflow:" + squidFlow.getName() + ",debug :" + s.getName());
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean shutdownSFTask(CharSequence taskId, CharSequence config) throws AvroRemoteException {
		log.debug("shutdownSFTask RPC 接收启动命令：taskId:" + taskId);
		// 判断是否处于暂停
		TSquidFlow squidFlow = EngineRPCServer.getInstance().getSquidFlow(taskId.toString());
		// squidflow 运行完成时返回为空
		if(squidFlow != null) {
			squidFlow.setSfFlag(false);
			for (TSquid s : squidFlow.getSquidList()) {
				if (s instanceof TDebugSquid) {
					TDebugSquid debugSquid = (TDebugSquid) s;
					if (debugSquid.isPause()) {
						debugSquid.release();
						log.debug("关闭前先继续运行该 squidflow:" + squidFlow.getName() + ",debug :" + s.getName());
						break;
					}
				}
			}
		}
		// 再关闭squidflow
		SquidFlowLauncher.stopSquidFlow(taskId.toString());
		for(TSquidFlow tsf : SquidFlowLauncher.runningSquidFlows) {
			if(taskId.equals(tsf.getTaskId())) {
				SquidFlowLauncher.runningSquidFlows.remove(tsf);
			}
		}
		return true;
	}

	@Override
	public void startSchedule(int jobId, CharSequence config) throws AvroRemoteException {
		log.debug("startSchedule RPC 接收启动命令：jobId:" + jobId);
		// todo 根据jobId，判断该job是否已经被调度了,如果调度了则不需要再启动
		/*ScheduleDao sd = ConstantUtil.getScheduleDao();
		SparkTask task = sd.getScheduleTaskById(jobId);
		ScheduleService.getInstance().addSchedule(task);*/
		log.debug("startSchedule RPC ======== 启动 =======job成功：jobId:" + jobId);
	}

	@Override
	public void stopSchedule(int jobId, CharSequence config) throws AvroRemoteException {
		log.debug("stopSchedule RPC 接收启动命令：jobId:" + jobId);
		//ScheduleService.getInstance().removeSchedule(jobId);
		// todo 关闭调度。
	}

	@Override
	public CharSequence take(CharSequence taskId, int debugSquidId, int num, CharSequence config) throws AvroRemoteException {
		TDebugSquid debugSquid = getSquidFlow(taskId.toString(), debugSquidId);
		if (debugSquid == null)
			return "";
		// return JsonUtil.toJSONString(debugSquid.take(num));
		return debugSquid.take(num).toString();
	}

	@Override
	public CharSequence takeSample(CharSequence taskId, int debugSquidId, boolean withReplacement, int num, int seed, CharSequence config) throws AvroRemoteException {
		TDebugSquid debugSquid = getSquidFlow(taskId.toString(), debugSquidId);
		if (debugSquid == null)
			return "";
		// return JsonUtil.toJSONString(debugSquid.takeSample(withReplacement,
		// num, seed));
		return (debugSquid.takeSample(withReplacement, num, seed)).toString();
	}

	/**
	 * 返回引擎的信息
	 * {"version":"","serverIp":""}
	 * @return
	 * @throws AvroRemoteException
	 */
	@Override
	public CharSequence engineInfo() throws AvroRemoteException {
		Map<String, String> map = new HashMap<>();
		map.put("version", ConfigurationUtil.version());
		map.put("serverIp", ConfigurationUtil.getServerRpcServerIp());
		return JSON.toJSONString(map);

	}

	@Override
	public CharSequence test(CharSequence test) throws AvroRemoteException {
		log.info("测试......................." + test);
		return test;
	}

	private TDebugSquid getSquidFlow(String taskId, int debugSquidId) {
		TSquidFlow squidFlow = EngineRPCServer.getInstance().getSquidFlow(taskId);
		if (squidFlow == null)
			return null;
		for (TSquid s : squidFlow.getSquidList()) {
			if (s.getSquidId() == debugSquidId) {
				return (TDebugSquid) s;
			}
		}
		return null;
	}

	/**
	 * config: 0=>开始新的任务  2=>从暂停中恢复
	 * @param jobIds
	 * @param config
	 * @throws AvroRemoteException
	 */
	@Override
	public void startSchedules(CharSequence jobIds, CharSequence config) throws AvroRemoteException {
		log.debug("startSchedule RPC 接收启动命令：jobId:" + jobIds);
		for(String strJobId : jobIds.toString().split(",")){
			ScheduleJob job = ScheduleService.getInstance().getScheduleJobById(Integer.parseInt(strJobId));
			try {
				//开启新的任务
				//if(Integer.parseInt(config.toString())==0){
					ScheduleJobManager.addCronJob(job,EngineScheduleJob.class);
				/*} else if(Integer.parseInt(config.toString())==2){
					//从暂停中恢复
					ScheduleJobManager.resumeJob(job.getId()+"");
				}*/
			} catch (SchedulerException e) {
				e.printStackTrace();
				log.error("开启调度任务出错:"+e);
			}
			log.debug("startSchedule RPC ======== 启动 =======job成功：jobId:" + Integer.parseInt(strJobId));
		}

	}

    @Override
    public void stopSchedules(CharSequence jobIds, CharSequence config) throws AvroRemoteException {
    	log.debug("stopSchedule RPC 接收启动命令：jobId:" + jobIds);
    	for(String strJobId : jobIds.toString().split(",")){
			ScheduleJob job = ScheduleService.getInstance().getScheduleJobById(Integer.parseInt(strJobId));
			try {
				if(job==null){
					//删除任务
					ScheduleJobManager.deleteJob(strJobId);
				} else {
					//if (job.getJob_status() == 0) {
						//停止任务
						ScheduleJobManager.stopJob(job.getId() + "");
					/*} else if (job.getJob_status() == 2) {
						//暂停
						ScheduleJobManager.pauseJob(job.getId() + "");
					}*/
				}
			} catch (SchedulerException e) {
				e.printStackTrace();
				log.error("开启调度任务出错:"+e);
			}
			log.debug("startSchedule RPC ======== 启动 =======job成功：jobId:" + Integer.parseInt(strJobId));
    		//ScheduleService.getInstance().removeSchedule(Integer.parseInt(strJobId));
    	}
    }

    @Override public CharSequence launchStreamJob(int squidFlowId, int repositoryId, CharSequence config)
            throws AvroRemoteException {
        log.info("================ 启动流式作业:" + repositoryId + "_" + squidFlowId + "_" + config);
        return EngineService.launchStreamJob(squidFlowId, repositoryId, config != null ? config.toString() : "{}");
    }

    @Override public CharSequence validateSquidFlow(int squidFlowId, CharSequence config)
            throws AvroRemoteException {
		SquidFlowDao dao = ConstantUtil.getSquidFlowDao();
		SquidFlow squidFlow = dao.getSquidFlow(squidFlowId);
		SquidFlowValidator squidFlowValidator = new SquidFlowValidator(squidFlow);
		Map<String,String> messages=squidFlowValidator.validate();
		if(messages.size()==0) {
			TransformationValidator transformationValidator = new TransformationValidator(squidFlow);
			return JSON.toJSONString(transformationValidator.validate());
		}else{
			return JSON.toJSONString(messages);
		}
    }

    @Override public boolean shutdownStreamJob(CharSequence appId, CharSequence config)
            throws AvroRemoteException {
        String appIdStr = appId.toString();
        log.info("停止流式作业:" + appIdStr + ", config:" + config);
        String[] strs = appIdStr.split("_");
        int userId = -1;
        if(StringUtils.isNotEmpty(config)) {
            JSONObject json = (JSONObject)JSONObject.parse(config.toString());
            Object uo = json.get("userid");
            if(uo != null) {
                userId = Integer.parseInt(uo.toString().trim());
            } else {
                log.error("参数缺失,没有用户ID");
            }
        }
        ApplicationStatusDao applicationStatusDao = ConstantUtil.getApplicationStatusDao();
        try {
            YarnClient client = YarnClient.createYarnClient();
            client.init(new Configuration());
            client.start();
            ApplicationId applicationId = ApplicationId.newInstance(Long.parseLong(strs[1]), Integer.parseInt(strs[2]));
            try {
                ApplicationReport report = client.getApplicationReport(applicationId);
                if(report.getYarnApplicationState() == YarnApplicationState.FINISHED
                        || report.getYarnApplicationState() == YarnApplicationState.FAILED
                        || report.getYarnApplicationState() == YarnApplicationState.KILLED) {
                    applicationStatusDao.updateKillApplicationStatus(appId.toString(), userId);
                    return true;
                }
            } catch (Exception e) {
                // application 没有找到
                if (e.getClass() == ApplicationNotFoundException.class) {
                    applicationStatusDao.updateKillApplicationStatus(appId.toString(), userId);
                    return true;
                }
            }
            client.killApplication(applicationId);
            client.stop();
            // 更新app状态
            applicationStatusDao.updateKillApplicationStatus(appId.toString(), userId);
            return true;
        } catch (Exception e) {
            log.error("关闭yarn 应用失败 ", e);
            return false;
        }
    }
}
