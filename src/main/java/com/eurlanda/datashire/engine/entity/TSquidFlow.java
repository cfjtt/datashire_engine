package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.common.rpc.server.EngineRPCServer;
import com.eurlanda.datashire.engine.entity.clean.Cleaner;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.service.SquidFlowLauncher;
import com.eurlanda.datashire.engine.util.EngineLogFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TSquidFlow implements Serializable {
    private static Log logger = LogFactory.getLog(TSquidFlow.class);
    private List<TSquid> squidList = new ArrayList<>();
    private Map<Integer, Object> result = null;//squidId and result
    private boolean debugModel = false;
    private TJobContext jobContext = new TJobContext();
    // 是否成功的标记,true 运行成功；可以运行   false:运行失败；不可以继续运行
    private boolean sfFlag = true;
    // 清理清单
    private List<Cleaner> cleaners = new ArrayList<>();

    /**
     * squidFlow 运行
     */
    public void run(JavaSparkContext jsc) throws EngineException {
        addSquidFlow();
        try {
            EngineLogFactory.logInfo(this, "launch job，taskId：" + getTaskId() + " .");
            for (TSquid s : squidList) {
                s.setCurrentFlow(this);
                logger.info("squid flow: squidId => " + s.getSquidId() + " " + s.getType());
                if (s.isFinished()) {
                    continue;
                }
                s.runSquid(jsc);
            }
        } finally {
            // 更改squidFlow job 的运行状态，如果为运行中则设置为成功
            removeSquidFlow();
            doClean();
        }
    }

    private void removeSquidFlow() {
        if (isDebugModel())
            EngineRPCServer.getInstance().removeSquidFlow(getTaskId());
        // todo 到时候优化这个流程
        TSquidFlow sf = null;
        for(TSquidFlow tsf : SquidFlowLauncher.runningSquidFlows) {
            if(tsf.getTaskId().equals(getTaskId())) {
                sf = tsf;
                break;
            }
        }
        if(sf != null) {
            SquidFlowLauncher.runningSquidFlows.remove(sf);
        }
    }

    public String getTaskId() {
        return jobContext.getTaskId();
    }

    private void addSquidFlow() {
        if (isDebugModel()) {
            EngineRPCServer.getInstance().addSquidFlow(this);
        } else {
            logger.info("debug model is off.");
        }
    }

    public void doClean() {
        if(sfFlag) {
            for(Cleaner c : cleaners) {
                c.doSuccess();
            }
        } else {
            for(Cleaner c : cleaners) {
                c.doFail();
            }
        }
    }


    public void addCleaner(Cleaner cleaner) {
        this.cleaners.add(cleaner);
    }

    public List<TSquid> getSquidList() {
        return squidList;
    }

    /**
     * deprecated by addSquid()
     */
    public void setSquidList(List<TSquid> squidList) {
        this.squidList = squidList;
    }

    public void addSquid(TSquid squid) {
        getSquidList().add(squid);
    }

    public Map<Integer, Object> getResult() {
        return result;
    }

    public void setResult(Map<Integer, Object> result) {
        this.result = result;
    }

    public boolean isDebugModel() {
        return debugModel && (!SquidFlowLauncher.local);
    }

    public void setDebugModel(boolean debugModel) {
        this.debugModel = debugModel;
    }

    public String getProjectName() {
        return jobContext.getProjectName();
    }

    public void setProjectName(String projectName) {
        jobContext.setProjectName(projectName);
    }

    public TJobContext getJobContext() {
        return jobContext;
    }

    public void setJobContext(TJobContext jobContext) {
        this.jobContext = jobContext;
    }

    public void setSfFlag(boolean sfFlag) {
        this.sfFlag = sfFlag;
    }

    public boolean isSfFlag() {
        return sfFlag;
    }

    public List<Cleaner> getCleaners() {
        return cleaners;
    }

    public String getName() {
        return jobContext.getSquidFlowName();
    }

    public int getRepositoryId() {
        return jobContext.getRepositoryId();
    }

    public String getRepositoryName() {
        return jobContext.getRepositoryName();
    }

    public int getId() {
        return jobContext.getSquidFlowId();
    }

    public void setTaskId(String taskId) {
        jobContext.setTaskId(taskId);
    }

    public void setRepositoryId(Integer repositoryId) {
        jobContext.setRepositoryId(repositoryId);
    }

    public void setRepositoryName(String repositoryName) {
        jobContext.setRepositoryName(repositoryName);
    }

    public void setId(Integer squidFlowId) {
        jobContext.setSquidFlowId(squidFlowId);
    }

    public void setJobId(int jobId) {
        jobContext.setJobId(jobId);
    }

    public void setName(String name) {
        jobContext.setSquidFlowName(name);
    }

    public void setProjectId(int projectId) {
        jobContext.setProjectId(projectId);
    }

    public int getJobId() {
        return jobContext.getJobId();
    }

    public int getProjectId() {
        return jobContext.getProjectId();
    }
    public int getUserId() {
        return jobContext.getUserId();
    }

    public void setUserId(int userId) {
        jobContext.setUserId(userId);
    }
    public int getMaxParallelNum() {
        return jobContext.getMaxParallelNum();
    }
    public void setMaxParallelNum(int maxParallelNum) {
        jobContext.setMaxParallelNum(maxParallelNum);
    }
    public int getMaxRunningTask() {
        return jobContext.getMaxRunningTask();
    }

    public void setMaxRunningTask(int maxRunningTask) {
        jobContext.setMaxRunningTask(maxRunningTask);
    }
    public int getHdfsSpaceLimit() {
        return jobContext.getHdfsSpaceLimit();
    }

    public void setHdfsSpaceLimit(int hdfsSpaceLimit) {
         jobContext.setHdfsSpaceLimit(hdfsSpaceLimit);
    }
    public String getJobName() {
        return jobContext.getJobName();
    }

    public void setJobName(String jobName) {
        jobContext.setJobName(jobName);
    }
}
