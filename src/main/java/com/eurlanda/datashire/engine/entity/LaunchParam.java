package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.common.webService.SquidEdit;

import java.util.List;
import java.util.Map;

/**
 * Created by zhudebin on 15-1-27.
 * 运行期参数
 */
public class LaunchParam {
    // squidflow id
    private int squidFlowId;
    // repositoryId
    private int repositoryId=0;
    // debug,dataview,destination
    private String ddvSquids;
    // 运行完邮件通知
    private String emails;
    // 运行完，HTTP 通知UTL
    private String notificationUrl;
    // 定时作业 JobId
    private Integer jobId = 0;
    // 允许同时多次重复运行
    private boolean allowMultipleRunning = false;

    private List<SquidEdit> squidEdits;

    private Map<String,String> variableEdits;

    public LaunchParam setSquidEdits(List<SquidEdit> squidEdits) {
        this.squidEdits = squidEdits;
        return this;
    }

    public LaunchParam setVariableEdits(Map<String, String> variableEdits) {
        this.variableEdits = variableEdits;
        return this;
    }

    public List<SquidEdit> getSquidEdits() {
        return squidEdits;
    }

    public Map<String, String> getVariableEdits() {
        return variableEdits;
    }

    public LaunchParam() {
    }

    public LaunchParam(String ddvSquids, String emails, String notificationUrl, Integer jobId, boolean allowMultipleRunning) {
        this.ddvSquids = ddvSquids;
        this.emails = emails;
        this.notificationUrl = notificationUrl;
        this.jobId = jobId;
        this.allowMultipleRunning = allowMultipleRunning;
    }

    public static LaunchParam builer() {
        return new LaunchParam();
    }

    public String getDdvSquids() {
        return ddvSquids;
    }

    public LaunchParam setDdvSquids(String ddvSquids) {
        this.ddvSquids = ddvSquids;
        return this;
    }

    public String getEmails() {
        return emails;
    }

    public LaunchParam setEmails(String emails) {
        this.emails = emails;
        return this;
    }

    public String getNotificationUrl() {
        return notificationUrl;
    }

    public LaunchParam setNotificationUrl(String notificationUrl) {
        this.notificationUrl = notificationUrl;
        return this;
    }

    public Integer getJobId() {
        return jobId;
    }

    public LaunchParam setJobId(Integer jobId) {
        this.jobId = jobId;
        return this;
    }

    public boolean isAllowMultipleRunning() {
        return allowMultipleRunning;
    }

    public LaunchParam setAllowMultipleRunning(boolean allowMultipleRunning) {
        this.allowMultipleRunning = allowMultipleRunning;
        return this;
    }

    public int getSquidFlowId() {
        return squidFlowId;
    }

    public LaunchParam setSquidFlowId(int squidFlowId) {
        this.squidFlowId = squidFlowId;
        return this;
    }

    public int getRepositoryId() {
        return repositoryId;
    }

    public LaunchParam setRepositoryId(int repositoryId) {
        this.repositoryId = repositoryId;
        return this;
    }
}
