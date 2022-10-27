package com.eurlanda.datashire.engine.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.eurlanda.datashire.common.webService.SquidEdit;
import com.eurlanda.datashire.engine.entity.LaunchParam;
import com.eurlanda.datashire.engine.entity.SFJobHistory;
import com.eurlanda.datashire.engine.entity.SFLog;
import com.eurlanda.datashire.engine.entity.TSquidFlow;
import com.eurlanda.datashire.engine.entity.clean.Cleaner;
import com.eurlanda.datashire.engine.entity.clean.NotificationCleaner;
import com.eurlanda.datashire.engine.entity.clean.SquidFlowEmailCleaner;
import com.eurlanda.datashire.engine.enumeration.JobStatusEnum;
import com.eurlanda.datashire.engine.enumeration.LogLevel;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.exception.EngineExceptionType;
import com.eurlanda.datashire.engine.schedule.ScheduleService;
import com.eurlanda.datashire.engine.translation.Translator;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.engine.util.EngineLogFactory;
import com.eurlanda.datashire.engine.util.UUIDUtil;
import com.eurlanda.datashire.entity.SquidFlow;
import com.eurlanda.datashire.server.model.ScheduleJob;
import org.apache.avro.AvroRuntimeException;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.deploy.yarn.StreamJobSubmit;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by zhudebin on 14-4-26.
 */
public class EngineService {
    private static Log log = LogFactory.getLog(EngineService.class);
    private static ExecutorService executor = Executors.newCachedThreadPool();
    /**
     * 非定时调度启动入口
     *
     * @param squidFlowId
     * @param repositoryId
     * @param ddvSquids
     * @return
     * @throws SQLException
     */
    public static String launchJob(int squidFlowId, int repositoryId, String ddvSquids) throws SQLException {

        /*SSquidFlowDao sSquidFlowDao = ConstantUtil.getBean(SSquidFlowDao.class);
        if(sSquidFlowDao.isStreamSquidFlow(squidFlowId)) {
            return launchStreamJob(squidFlowId, repositoryId);
        } else {
            LaunchParam param = LaunchParam.builer()
                    .setSquidFlowId(squidFlowId)
                    .setRepositoryId(repositoryId)
                    .setDdvSquids(ddvSquids);
            return launchJob(param);
        }*/

        LaunchParam param = LaunchParam.builer()
                .setSquidFlowId(squidFlowId)
                .setRepositoryId(repositoryId)
                .setDdvSquids(ddvSquids);
        return launchJob(param);
    }

    /**
     * 启动流式作业
     *
     * @param squidFlowId
     * @param repositoryId
     * @return
     */
    public static String launchStreamJob(int squidFlowId, int repositoryId, String config) {
        log.info("============= 启动作业 ========== " + squidFlowId + ",config:" + config);
        JSONObject json = JSONObject.parseObject(config);
        Iterator<Map.Entry<String, Object>> iter = json.entrySet().iterator();
        Map<String, String> configMap = new HashMap<>();
        while (iter.hasNext()) {
            Map.Entry<String, Object> entry = iter.next();
            configMap.put(entry.getKey(), entry.getValue().toString());
        }

        return StreamJobSubmit.submit(squidFlowId, 1000, configMap);
    }

    /**
     * 非定时调度启动入口
     *
     * @param squidFlowId
     * @param repositoryId
     * @return
     * @throws SQLException
     */
    public static String launchJob(int squidFlowId, int repositoryId) throws SQLException {
        LaunchParam param = LaunchParam.builer()
                .setSquidFlowId(squidFlowId)
                .setRepositoryId(repositoryId);
        return launchJob(param);
    }

    /**
     * 非定时调度启动入口
     *
     * @param squidFlowId
     * @param notificationUrl
     * @return
     * @throws SQLException
     */
    public static String launchJob(int squidFlowId, String notificationUrl) throws SQLException {
        LaunchParam param = LaunchParam.builer()
                .setSquidFlowId(squidFlowId)
                .setNotificationUrl(notificationUrl);
        return launchJob(param);
    }

    /**
     * 非定时调度启动入口
     *
     * @param squidFlowId
     * @param notificationUrl
     * @param squidEditList
     * @param variables
     * @return
     * @throws SQLException
     */
    public static String launchJob(int squidFlowId, String notificationUrl, List<SquidEdit> squidEditList,
                                   Map<String, String> variables) throws SQLException {
        LaunchParam param = LaunchParam.builer()
                .setSquidFlowId(squidFlowId)
                .setNotificationUrl(notificationUrl)
                .setSquidEdits(squidEditList)
                .setVariableEdits(variables);
        return launchJob(param);
    }

    /**
     * 非定时调度启动入口
     *
     * @param squidFlowId
     * @param notificationUrl
     * @return
     * @throws SQLException
     */
    public static String launchJob(int squidFlowId, String notificationUrl, boolean isMultiple) throws SQLException {
        LaunchParam param = LaunchParam.builer()
                .setSquidFlowId(squidFlowId)
                .setNotificationUrl(notificationUrl)
                .setAllowMultipleRunning(isMultiple);
        return launchJob(param);
    }

    /**
     * 定时调度启动方法
     *
     * @param squidFlowId
     * @param repositoryId
     * @param jobId
     * @return
     * @throws SQLException
     */
    public static String launchJob(int squidFlowId, int repositoryId, Integer jobId, String emails) throws SQLException {
        //允许重复运行
        LaunchParam param = LaunchParam.builer()
                .setSquidFlowId(squidFlowId)
                .setRepositoryId(repositoryId)
                .setJobId(jobId).setEmails(emails).setAllowMultipleRunning(true);
        return launchJob(param);
    }

    /**
     * 0. 生成taskId(UUID)
     * 1. 根据 repositoryId，squidFlowId 获取 squidFlow
     * 2. 翻译squidFlow -> TSquidFlow
     * 3. 启动 TSquidFlow
     * 4. 返回 taskId
     *
     * @param param
     * @return
     * @throws SQLException
     */
    public static String launchJob(LaunchParam param) throws SQLException {
        log.info("squidFlowId:" + param.getSquidFlowId() + ",repositoryId:" + param.getRepositoryId() + ",ddvSquids:" + param.getDdvSquids() + ",jobId:" + param.getJobId());
        // 判断该squid flow 是否正在运行，在运行，则直接返回taskId,否则启动后返回taskId
        final String taskId;
        TSquidFlow tsf;
        if (!param.isAllowMultipleRunning() && (tsf = isJobRunning(param.getRepositoryId(), param.getSquidFlowId())) != null) {
            SFLog sfLog = new SFLog();
            sfLog.setLogLevel(LogLevel.INFO.value);
            sfLog.setRepositoryId(param.getRepositoryId());
            sfLog.setSquidFlowId(param.getSquidFlowId());
            sfLog.setTaskId(tsf.getTaskId());
            sfLog.setMessage("job is running, taskId:" + tsf.getTaskId() + ", repositoryId:" + param.getRepositoryId() + ",squidFlowId:" + param.getSquidFlowId());
            EngineLogFactory.log(sfLog);
            log.info("job is running, taskId:" + tsf.getTaskId() + ", repositoryId:" + param.getRepositoryId() + ",squidFlowId:" + param.getSquidFlowId());
            // 运行中 直接返回
            addCleaner(param, tsf);
            return tsf.getTaskId();
        } else {
            // 没有运行
            taskId = UUIDUtil.genUUID();
            TSquidFlow tSquidFlow = null;
            try {
                long ftime = System.currentTimeMillis();
                tSquidFlow = Translator.buildProxy(taskId, param.getJobId(),
                        param.getRepositoryId(), param.getSquidFlowId(),
                        param.getDdvSquids(), param.getSquidEdits(),
                        param.getVariableEdits());
                log.info("squidFlowId:" + param.getSquidFlowId()
                        + ",repositoryId:" + param.getRepositoryId()
                        + ",ddvSquids:" + param.getDdvSquids()
                        + " translate success，time-consuming(second)："
                        + (System.currentTimeMillis() - ftime) / 1000.0);
            } catch (Exception e) {
                //当翻译出错时，发送运行失败邮件
                tSquidFlow = new TSquidFlow();
                tSquidFlow.setTaskId(taskId);
                addCleanerWhenTranslateSquidFlowError(param, tSquidFlow);
                tSquidFlow.doClean();
                EngineLogFactory.logError(param.getSquidFlowId(), param.getRepositoryId(), param.getDdvSquids(), param.getJobId(), taskId, e);
                if(e  instanceof EngineException){
                    if(((EngineException) e).getType() == EngineExceptionType.UNION_COLUMN_SIZE_IS_NOT_EQUALS){
                        throw new AvroRuntimeException(EngineExceptionType.UNION_COLUMN_SIZE_IS_NOT_EQUALS.getCode()+"");
                    }
                } else {
                    throw new RuntimeException(e);
                }
            }
            String pool = null;
            try {
                //插入执行记录
                insertSFJobHistory(param.getSquidFlowId(), param.getRepositoryId(), param.getDdvSquids(), param.getJobId(), taskId);
                addCleaner(param, tSquidFlow);

                //判断当前是否还有达到最大并发数(达到了加入到等待队列)
                boolean flag = SquidFlowLauncher.isParallelMax(tSquidFlow);
                pool = SquidFlowLauncher.getNewPool(tSquidFlow,flag);
                if (com.eurlanda.datashire.utility.StringUtils.isNotNull(pool)) {
                    //运行spark job
                    SquidFlowLauncher.launch(tSquidFlow);
                }

            } catch (EngineException e) {
                log.error("launch job error", e);
                EngineLogFactory.logError(param.getSquidFlowId(), param.getRepositoryId(), param.getDdvSquids(), param.getJobId(), taskId, e);
                ConstantUtil.getSFJobHistoryDao()
                        .updateStatus(taskId, param.getSquidFlowId(), JobStatusEnum.FAIL);
                return null;
            }
            if (com.eurlanda.datashire.utility.StringUtils.isNotNull(pool)) {
                EngineLogFactory.logInfo(tSquidFlow, "launch job success,taskId:" + taskId);
            }
//            log.info("启动作业成功,taskId:" + taskId + ", repositoryId:" + param.getRepositoryId() + ",squidFlowId:" + param.getSquidFlowId());
            return taskId;
        }
    }

    private static void insertSFJobHistory(int squidFlowId, int repositoryId, String ddvSquids, Integer jobId, String taskId) {
        // 设置squidflow job 运行状态为 运行中
        SFJobHistory sfJob = new SFJobHistory();
        sfJob.setTaskId(taskId);
        sfJob.setJobId(jobId != null ? jobId : -1);
        sfJob.setRepositoryId(repositoryId);
        sfJob.setSquidFlowId(squidFlowId);
        sfJob.setStatus(JobStatusEnum.RUNNING.value);
        Map<String, Object> paraMap = (Map<String, Object>) JSON.parse(ddvSquids);
        if (MapUtils.isNotEmpty(paraMap)) {
            if (paraMap.get("breakPoints") != null)
                sfJob.setDebugSquids(paraMap.get("breakPoints").toString());
            if (paraMap.get("destinations") != null)
                sfJob.setDestinationSquids(paraMap.get("destinations").toString());
            if (paraMap.get("dataViewers") != null)
                sfJob.setDataViewSquids(paraMap.get("dataViewers").toString());
        }
        ConstantUtil.getSFJobHistoryDao().insert(sfJob);
    }

    // 状态：运行中0、成功1、失败-1
    private static TSquidFlow isJobRunning(Integer repositoryId, Integer squidFlowId) {
        boolean is_validate = false;
        if (is_validate) {
            for (TSquidFlow tsf : SquidFlowLauncher.runningSquidFlows) {
                if (tsf.getId() == squidFlowId.intValue()) {
                    log.info("该squidflow 正在运行中....");
                    return tsf;
                }
            }
        }
        return null;
    }

    private static void addCleaner(LaunchParam param, TSquidFlow tsf) {
        String emails = param.getEmails();
        String notificationUrl = param.getNotificationUrl();
        if (StringUtils.isNotEmpty(emails)) {
            addEmailCleaner(emails, tsf);
        }
        if (StringUtils.isNotEmpty(notificationUrl)) {
            addNotificationCleaner(notificationUrl, tsf);
        }
    }

   private static void addCleanerWhenTranslateSquidFlowError(LaunchParam param,TSquidFlow tsf){
       try {
           SquidFlow sf = Translator.getSquidFlow(param.getRepositoryId(), param.getSquidFlowId(),param.getSquidEdits());
           tsf.setRepositoryId(param.getRepositoryId());
           tsf.setTaskId(tsf.getTaskId());
           tsf.setSfFlag(false);
           tsf.setId(param.getSquidFlowId());
           tsf.setProjectId(sf.getProject_id());
           tsf.setJobId(param.getJobId());
           //获取jobName
           if(param.getJobId()!=null && param.getJobId()>0){
               ScheduleJob job = ScheduleService.getInstance().getScheduleJobById(param.getJobId());
               if(job!=null){
                   tsf.setJobName(job.getName());
               }
           }
           //获取projectName等信息
           Translator.setTSquidFlowInfo(tsf);
       } catch (IllegalAccessException e) {
           e.printStackTrace();
       }
       String emails = param.getEmails();
        String notificationUrl = param.getNotificationUrl();
        if (StringUtils.isNotEmpty(emails)) {
            addEmailCleaner(emails, tsf);
        }
        if (StringUtils.isNotEmpty(notificationUrl)) {
            addNotificationCleaner(notificationUrl, tsf);
        }
    }

    /**
     * 为squidflow添加邮件通知
     * 如果该squidflow中已经存在同一个调度产生的邮件通知就不再添加，
     * 防止调度过于频繁，一次运行受到多封邮件
     * 因为squidflow一个时间点只会运行一次，
     *
     * @param tsf
     * @param emails
     */
    private static void addEmailCleaner(String emails, TSquidFlow tsf) {
        for (Cleaner cleaner : tsf.getCleaners()) {
            if (cleaner instanceof SquidFlowEmailCleaner) {
                SquidFlowEmailCleaner sec = (SquidFlowEmailCleaner) cleaner;
                if (emails.equals(sec.getEmails())) {
                    return;
                }
            }
        }
        tsf.addCleaner(new SquidFlowEmailCleaner(emails, tsf));
    }

    private static void addNotificationCleaner(String notificationUrl, TSquidFlow tsf) {
        for (Cleaner cleaner : tsf.getCleaners()) {
            if (cleaner instanceof NotificationCleaner) {
                NotificationCleaner sec = (NotificationCleaner) cleaner;
                if (notificationUrl.equals(sec.getUrl())) {
                    return;
                }
            }
        }
        tsf.addCleaner(new NotificationCleaner(notificationUrl, tsf));
    }
}
