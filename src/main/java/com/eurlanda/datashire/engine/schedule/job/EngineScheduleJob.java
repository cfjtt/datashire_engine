package com.eurlanda.datashire.engine.schedule.job;

import com.eurlanda.datashire.engine.service.EngineService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.InterruptableJob;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.UnableToInterruptJobException;

import java.sql.SQLException;
import java.util.Map;

/**
 调度
 */
public class EngineScheduleJob implements InterruptableJob {

    private static Log log = LogFactory.getLog(EngineScheduleJob.class);
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        Map<String,Object> map = context.getJobDetail().getJobDataMap();
        int squidFlowId = Integer.parseInt(map.get("squidFlowId")+"");
        int enableEmail = Integer.parseInt(map.get("enableEmail")+"");
        String email = null;
        if(enableEmail==1){
            email = map.get("email")+"";
        }
        int jobId = Integer.parseInt(map.get("jobId")+"");
        int repositoryId = Integer.parseInt(map.get("repositoryId")+"");
        try {
            log.info("调度squidflow:"+squidFlowId+" 开始运行");
            EngineService.launchJob(squidFlowId,repositoryId,jobId,email);
        } catch (SQLException e) {
            e.printStackTrace();
            log.error(e);
        }
    }

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        log.info("停止任务");
    }
}
