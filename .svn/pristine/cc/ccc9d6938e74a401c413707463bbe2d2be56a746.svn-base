package com.eurlanda.datashire.engine.entity;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * Created by Juntao.Zhang on 2014/5/28.
 */
public class TJobContext implements Serializable {
    private int jobId;
    private String jobName;
    private String taskId;
    private int repositoryId;
    private String repositoryName;
    private int projectId;
    private String projectName;
    private int squidFlowId;
    private String squidFlowName;

    private int squidId;
    private String squidName;
    private String tSquidId;
    private int tSquidType;

    private int userId;  //用户的id(个人id/数猎场Id/本地)
    private int maxParallelNum; //允许的运行数
    private int maxRunningTask;  //单个任务允许的最大task

    private int hdfsSpaceLimit;  //hdfs落地容量限制
    private transient SparkSession sparkSession;

    public void from(TSquid squid){
        this.setSquidId(squid.getSquidId());
        this.setSquidName(squid.getName());
//        this.setSquidName(squid.getType().name());
        this.settSquidId(squid.getId());
        this.settSquidType(squid.getType().value());
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

    public String gettSquidId() {
        return tSquidId;
    }

    public void settSquidId(String tSquidId) {
        this.tSquidId = tSquidId;
    }

    public int gettSquidType() {
        return tSquidType;
    }

    public void settSquidType(int tSquidType) {
        this.tSquidType = tSquidType;
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

    public SQLContext getSqlContext() {
        return sparkSession.sqlContext();
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public void setSparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public int getMaxParallelNum() {
        return maxParallelNum;
    }

    public void setMaxParallelNum(int maxParallelNum) {
        this.maxParallelNum = maxParallelNum;
    }

    public int getMaxRunningTask() {
        return maxRunningTask;
    }

    public void setMaxRunningTask(int maxRunningTask) {
        this.maxRunningTask = maxRunningTask;
    }

    public int getHdfsSpaceLimit() {
        return hdfsSpaceLimit;
    }

    public void setHdfsSpaceLimit(int hdfsSpaceLimit) {
        this.hdfsSpaceLimit = hdfsSpaceLimit;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }
}
