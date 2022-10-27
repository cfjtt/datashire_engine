package com.eurlanda.datashire.engine.schedule;

import com.eurlanda.datashire.engine.schedule.job.EngineScheduleJob;
import com.eurlanda.datashire.engine.util.ConfigurationUtil;
import com.eurlanda.datashire.server.model.ScheduleJob;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;

import java.util.*;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

/**
 * Created by zhudebin on 2017/1/2.
 */
public class ScheduleJobManager {

    private static Log log = LogFactory.getLog(ScheduleJobManager.class);

    private static ScheduleJobManager manager = getInstance();
    private static SchedulerFactory sf = getSchedulerFactory();
    private boolean isStarted = false;

    private ScheduleJobManager() {
    }

    public static ScheduleJobManager getInstance() {
        if(manager == null){
            manager = new ScheduleJobManager();
        }
        return manager;
    }
    public static SchedulerFactory getSchedulerFactory(){
        if(sf==null) {
            try {
                sf = new StdSchedulerFactory(ConfigurationUtil.getQuartProperties());
            } catch (SchedulerException e) {
                e.printStackTrace();
            }
        }
        return sf;
    }
    /**
     * 系统启动的时候，扫描数据库，job
     */
    public void startSchedule() {
        if(isStarted) {     // 只能允许启动一个
            return;
        }
        try {
            ScheduleService service = ScheduleService.getInstance();
            List<ScheduleJob> jobs = service.getScheduleJobs();
            Scheduler sched = sf.getScheduler();
            sched.start();
            for(ScheduleJob job : jobs){
                try {
                    if(StringUtils.isNotNull(job.getCron_expression())){
                        addCronJob(job, EngineScheduleJob.class);
                    }
                } catch (Exception e){
                    e.printStackTrace();
                    log.error("启动时，添加调度作业异常");
                }
            }
        } catch (SchedulerException e) {
            e.printStackTrace();
        }
        isStarted = true;
    }

    public void addJob(Scheduler sched, String cron, String jobname, Class<? extends Job> jobClass)
            throws SchedulerException {
        JobDetail job = newJob(jobClass)
                .withIdentity(jobname, jobname)
                .build();
        CronTrigger trigger = newTrigger()
                .withIdentity(jobname, jobname)
                .withSchedule(cronSchedule(cron))
                .build();
        Date ft = sched.scheduleJob(job, trigger);
        sched.start();
        log.info(job.getKey() + " has been scheduled to run at: " + ft
                + " and repeat based on expression: "
                + trigger.getCronExpression());
    }

    /**
     * 添加CronTrigger任务（复杂的调度任务，例如每月第几天，每周的第几天等等）
     * @param jobClass
     * @throws SchedulerException
     */
    public static void addCronJob(ScheduleJob scheduleJob,Class<? extends Job> jobClass) throws SchedulerException {
        Scheduler sched = sf.getScheduler();
        Map<String,Object> jobParamMap = new HashMap<>();
        jobParamMap.put("squidFlowId",scheduleJob.getSquid_flow_id());
        jobParamMap.put("enableEmail",scheduleJob.getEnable_email());
        jobParamMap.put("email",scheduleJob.getEmail_address());
        jobParamMap.put("jobId",scheduleJob.getId());
        jobParamMap.put("repositoryId",scheduleJob.getRepository_id());
        JobDataMap map = new JobDataMap(jobParamMap);
        JobDetail job = newJob(jobClass)
                .withIdentity(scheduleJob.getId()+"", scheduleJob.getId()+"").setJobData(map)
                .build();
        CronTrigger trigger = newTrigger()
                .withIdentity(scheduleJob.getId()+"", scheduleJob.getId()+"")
                .withSchedule(cronSchedule(scheduleJob.getCron_expression()).withMisfireHandlingInstructionFireAndProceed())
                .build();
        Date ft = sched.scheduleJob(job, trigger);
        if(!sched.isStarted()){
            sched.start();
        }
        log.info("squidflowId:"+scheduleJob.getSquid_flow_id()+"开始加入调度任务");
        log.info(job.getKey() + " has been scheduled to run at: " + ft
                + " and repeat based on expression: "
                + trigger.getCronExpression());
    }

    /**
     * 添加简单规则的调度任务(固定时间执行，每个多长时间执行一次等等)
     * @param sched
     * @param trigger
     * @param jobname
     * @param jobClass
     * @throws SchedulerException
     */
    public static void addSimpleTriggerJob(Scheduler sched,Trigger trigger,String jobname,Class<? extends Job> jobClass) throws SchedulerException {
        JobDetail job = newJob(jobClass)
                .withIdentity(jobname, jobname)
                .build();
        sched.scheduleJob(job, trigger);
    }

    /**
     * 删除任务
     * @param jobName
     */
    public static void deleteJob(String jobName) throws SchedulerException {
        Scheduler sched = sf.getScheduler();
        log.info("开始删除调度任务:"+jobName);
        boolean flag = sched.deleteJob(JobKey.jobKey(jobName,jobName));
        if(flag){
            log.info("删除调度任务成功");
        } else {
            log.info("删除调度任务失败");
        }

    }

    /**
     * 暂停任务
     * @param jobName
     */
    public static void pauseJob(String jobName) throws SchedulerException {
        Scheduler sched = sf.getScheduler();
        log.info("开始暂停调度任务:"+jobName);
        sched.pauseJob(JobKey.jobKey(jobName,jobName));
        log.info("暂停调度任务成功");
    }

    /**
     * 恢复任务
     * @param jobName
     */
    public static void resumeJob(String jobName) throws SchedulerException {
        Scheduler sched = sf.getScheduler();
        log.info("开始恢复任务:"+jobName);
        sched.resumeJob(JobKey.jobKey(jobName,jobName));
        log.info("恢复任务成功");
    }

    /**
     * 停止任务
     * @param jobName
     * @throws SchedulerException
     */
    public static void stopJob(String jobName) throws SchedulerException {
        Scheduler sched = sf.getScheduler();
        log.info("开始停止任务:"+jobName);
        boolean flag = sched.deleteJob(JobKey.jobKey(jobName,jobName));
        if(flag){
            log.info("停止任务成功");
        } else {
            log.info("停止任务失败");
        }
    }

    /**
     * 判断该job是否是暂停的状态
     * @param jobName
     * @throws SchedulerException
     */
    public static boolean isPauseJob(String jobName) throws SchedulerException {
        Scheduler sched = sf.getScheduler();
        Set<String> parseJobs = sched.getPausedTriggerGroups();
        for(String id : parseJobs){
            if(id.equals(jobName)){
                return true;
            }
        }
        return false;
    }
    /**
     * 获取job
     * @param jobName
     * @return
     * @throws SchedulerException
     */
    public static Set<JobKey> getJobs(String jobName) throws SchedulerException {
        Scheduler sched = sf.getScheduler();
        Set<JobKey> jobs = new HashSet<>();
        if(jobName==null){
            jobs = sched.getJobKeys(GroupMatcher.anyJobGroup());
        } else {
            jobs = sched.getJobKeys(GroupMatcher.jobGroupEquals(jobName));
        }
        return jobs;
    }


    public static void main(String[] args){

    }
}
