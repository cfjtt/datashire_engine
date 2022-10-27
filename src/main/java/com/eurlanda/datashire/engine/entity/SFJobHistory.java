package com.eurlanda.datashire.engine.entity;

import java.util.Date;

/**
 * Created by zhudebin on 14-5-6.
 * squidflow job 运行日志
 */
public class SFJobHistory {
    // 运行任务ID
    private String taskId;
    private int jobId;
    // 仓库ID
    private int repositoryId;
    // squidflow Id
    private int squidFlowId;
    // squid flow 运行状态
    private int status;
    // debug squid
    private String debugSquids;
    // destination squid
    private String destinationSquids;
    // dataview squid
    private String dataViewSquids;
    // 调用者（调度的显示调度JOBID）
    private String caller;
    //  创建时间
    private Date createTime;
    // 更新时间
    private Date updateTime;

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public int getRepositoryId() {
        return repositoryId;
    }

    public void setRepositoryId(int repositoryId) {
        this.repositoryId = repositoryId;
    }

    public int getSquidFlowId() {
        return squidFlowId;
    }

    public void setSquidFlowId(int squidFlowId) {
        this.squidFlowId = squidFlowId;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getDebugSquids() {
        return debugSquids;
    }

    public void setDebugSquids(String debugSquids) {
        this.debugSquids = debugSquids;
    }

    public String getDestinationSquids() {
        return destinationSquids;
    }

    public void setDestinationSquids(String destinationSquids) {
        this.destinationSquids = destinationSquids;
    }

    public String getCaller() {
        return caller;
    }

    public void setCaller(String caller) {
        this.caller = caller;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public String getDataViewSquids() {
        return dataViewSquids;
    }

    public int getJobId() {
        return jobId;
    }

    public void setJobId(int jobId) {
        this.jobId = jobId;
    }

    public void setDataViewSquids(String dataViewSquids) {
        this.dataViewSquids = dataViewSquids;
    }
}
