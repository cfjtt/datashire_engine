package com.eurlanda.datashire.engine.entity.clean;

import com.eurlanda.datashire.engine.entity.TSquidFlow;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by zhudebin on 15-1-20.
 */
public class NotificationCleaner extends Cleaner {
    private static Log log = LogFactory.getLog(NotificationCleaner.class);
    private String url;

    private TSquidFlow tsf;

    public NotificationCleaner(String url, TSquidFlow tsf) {
        this.url = url;
        this.tsf = tsf;
    }

    /**
     * 成功了清理
     */
    public void doSuccess() {
        callBackNotice(tsf.getTaskId(),true,url);
    }

    /**
     * 失败了清理
     */
    public void doFail() {
        callBackNotice(tsf.getTaskId(), false, url);
    }

    private void callBackNotice(String taskId,boolean isSuccess,String url) {
        try {
            int status = 101;
            if(isSuccess){
                status=0;
            }
            PostMethod postMethod = new PostMethod(url);
            HttpClient httpClient = new HttpClient();
            postMethod.setParameter("taskId", taskId);
            postMethod.setParameter("status", status+"");
            int statusCode = httpClient.executeMethod(postMethod);
            if(statusCode==200){
                log.info("URL 回调任务:" + taskId + ";调用成功");
            }else{
                log.info("URL 回调任务:" + taskId + ";调用成功");
            }
        } catch (Exception e) {
            log.error("URL 回调任务:" + taskId + " 异常 ",e);
        }
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public TSquidFlow getTsf() {
        return tsf;
    }

    public void setTsf(TSquidFlow tsf) {
        this.tsf = tsf;
    }
}
