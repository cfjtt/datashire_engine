package com.eurlanda.datashire.engine.entity;

/**
 * squid flow 生成的调度作业运行日志(该表不在hsqldb中，在引擎的habase中)
 * Created by Juntao.Zhang on 2014/5/28.
 */
public class SFLog {
    private Long id;
    private int jobId;
    private int repositoryId;
    private String repositoryName;
    private int projectId;
    private String projectName;
    private int squidFlowId;
    private String squidFlowName;
    private int squidId;
    private String squidName;
    private String taskId;
    private String message;
    private int logLevel;
    private long createTime = System.currentTimeMillis();
    private String tSquidId;
    private int tSquidType;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public int getJobId() {
        return jobId;
    }

    public void setJobId(int jobId) {
        this.jobId = jobId;
    }

    public int getRepositoryId() {
        return repositoryId;
    }

    public void setRepositoryId(int repositoryId) {
        this.repositoryId = repositoryId;
    }

    public String getRepositoryName() {
        return repositoryName;
    }

    public void setRepositoryName(String repositoryName) {
        this.repositoryName = repositoryName;
    }

    public int getProjectId() {
        return projectId;
    }

    public void setProjectId(int projectId) {
        this.projectId = projectId;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public int getSquidFlowId() {
        return squidFlowId;
    }

    public void setSquidFlowId(int squidFlowId) {
        this.squidFlowId = squidFlowId;
    }

    public String getSquidFlowName() {
        return squidFlowName;
    }

    public void setSquidFlowName(String squidFlowName) {
        this.squidFlowName = squidFlowName;
    }

    public int getSquidId() {
        return squidId;
    }

    public void setSquidId(int squidId) {
        this.squidId = squidId;
    }

    public String getSquidName() {
        return squidName;
    }

    public void setSquidName(String squidName) {
        this.squidName = squidName;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public int getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(int logLevel) {
        this.logLevel = logLevel;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public String getTSquidId() {
        return tSquidId;
    }

    public void setTSquidId(String tSquidId) {
        this.tSquidId = tSquidId;
    }

    public int getTSquidType() {
        return tSquidType;
    }

    public void setTSquidType(int tSquidType) {
        this.tSquidType = tSquidType;
    }

    public void from(TJobContext jobContext) {
        this.setJobId(jobContext.getJobId());
        this.setTaskId(jobContext.getTaskId());
        this.setProjectId(jobContext.getProjectId());
        this.setProjectName(jobContext.getProjectName());
        this.setRepositoryId(jobContext.getRepositoryId());
        this.setRepositoryName(jobContext.getRepositoryName());
        this.setSquidId(jobContext.getSquidId());
        this.setSquidName(jobContext.getSquidName());
        this.setTSquidId(jobContext.gettSquidId());
        this.setSquidFlowId(jobContext.getSquidFlowId());
        this.setSquidFlowName(jobContext.getSquidFlowName());
        this.setTSquidType(jobContext.gettSquidType());
    }
}
