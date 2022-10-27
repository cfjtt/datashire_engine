package com.eurlanda.datashire.engine.entity.clean;

import com.eurlanda.datashire.engine.entity.TSquidFlow;
import com.eurlanda.datashire.engine.util.EmailSenderUtil;
import com.eurlanda.datashire.engine.util.EngineLogFactory;

/**
 * Created by zhudebin on 14-10-30.
 */
public class SquidFlowEmailCleaner extends Cleaner {

    private String emails;

    private TSquidFlow tsf;

    public SquidFlowEmailCleaner(String emails, TSquidFlow tsf) {
        this.emails = emails;
        this.tsf = tsf;
    }

    @Override
    public void doFail() {
        // squidflow 运行失败，发送邮件
        try {
            String text = "";
            if(tsf.getJobId()>0){
                text = tsf.getRepositoryName() + "->" + tsf.getProjectName() + "->" + tsf.getName() + "->"+tsf.getJobName()+" 运行失败";
            } else {
                text = tsf.getRepositoryName() + "->" + tsf.getProjectName() + "->" + tsf.getName() + " 运行失败";
            }
            EmailSenderUtil.sendMailBySynchronizationMode(
                    new EmailSenderUtil.Mail(emails.split(";"), null,
                            "squidflow operation log", text, false));
            EngineLogFactory.logInfo(tsf, " Send mail success(job run failed)");
        } catch (Exception e) {
            e.printStackTrace();
            EngineLogFactory.logError(tsf, new RuntimeException("send mail failed", e));
        }
    }

    @Override
    public void doSuccess() {
        // squidflow 运行成功，发送邮件
        try {
            String text = "";
            if(tsf.getJobId()>0){
                text = tsf.getRepositoryName() + "->" + tsf.getProjectName() + "->" + tsf.getName() + "->"+tsf.getJobName()+" 运行成功";
            } else {
                text = tsf.getRepositoryName() + "->" + tsf.getProjectName() + "->" + tsf.getName() + " 运行成功";
            }
            EmailSenderUtil.sendMailBySynchronizationMode(
                    new EmailSenderUtil.Mail(emails.split(";"), null,
                            "squidflow operation log", text, false));
            EngineLogFactory.logInfo(tsf, "Send mail success(job run success)");
        } catch (Exception e) {
            e.printStackTrace();
            EngineLogFactory.logError(tsf, new RuntimeException("send mail failed", e));
        }
    }

    public String getEmails() {
        return emails;
    }

    public void setEmails(String emails) {
        this.emails = emails;
    }

    public TSquidFlow getTsf() {
        return tsf;
    }

    public void setTsf(TSquidFlow tsf) {
        this.tsf = tsf;
    }
}
