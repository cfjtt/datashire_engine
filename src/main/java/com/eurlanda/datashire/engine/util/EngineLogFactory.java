package com.eurlanda.datashire.engine.util;

import com.eurlanda.datashire.engine.entity.SFLog;
import com.eurlanda.datashire.engine.entity.TJobContext;
import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.engine.entity.TSquidFlow;
import com.eurlanda.datashire.engine.enumeration.LogLevel;
import com.eurlanda.datashire.engine.kafka.LogProducer;

/**
 * Created by Juntao.Zhang on 2014/5/28.
 */
public class EngineLogFactory {


    public static void logError(int squidFlowId, int repositoryId, String ddvSquids, int jobId, String taskId, Exception e) {

        SFLog log = new SFLog();
        log.setSquidFlowId(squidFlowId);
        log.setRepositoryId(repositoryId);
        log.setJobId(jobId);
        log.setTaskId(taskId);
        log.setLogLevel(LogLevel.ERROR.value);

        log.setMessage("[ddvSquids:" + ddvSquids + "],[EngineException:" + exceptionStackMessage(e) + "]");
        LogProducer.sendLog(log);
    }

    public static String exceptionStackMessage(Exception e) {
        StringBuilder sb = new StringBuilder();
        sb.append(e.getMessage()).append("\n");
//        for(StackTraceElement ste : e.getStackTrace()) {
//            sb.append(ste.toString()).append("\n");
//        }
        return sb.toString();
    }

    public static void log(SFLog sfLog) {
        LogProducer.sendLog(sfLog);
    }

    public static void logInfo(TSquidFlow tSquidFlow, String message) {
        SFLog log = new SFLog();
        log.from(tSquidFlow.getJobContext());
        log.setMessage(message);
        log.setLogLevel(LogLevel.INFO.value);
        LogProducer.sendLog(log);
    }

    public static void logDebug(TSquid s, String message) {
        SFLog log = new SFLog();
        log.from(s.getJobContext());
        log.setMessage(message);
        log.setLogLevel(LogLevel.DEBUG.value);
        LogProducer.sendLog(log);
    }

    public static void logInfo(TSquid s, String message) {
        SFLog log = new SFLog();
        log.from(s.getJobContext());
        log.setMessage(message);
        log.setLogLevel(LogLevel.INFO.value);
        LogProducer.sendLog(log);
    }

    public static void logError(TSquid s, Exception e) {
        SFLog log = new SFLog();
        log.from(s.getJobContext());
        log.setMessage("{EngineException:" + exceptionStackMessage(e) + "}");
        log.setLogLevel(LogLevel.ERROR.value);
        LogProducer.sendLog(log);
    }

    public static void logError(TSquid s, String errorMsg) {
        SFLog log = new SFLog();
        log.from(s.getJobContext());
        log.setMessage("{Engine error:" + errorMsg + "}");
        log.setLogLevel(LogLevel.ERROR.value);
        LogProducer.sendLog(log);
    }

    public static void logError(TJobContext jobContext, Exception e) {
        SFLog log = new SFLog();
        log.from(jobContext);
        log.setMessage("{EngineException:" + exceptionStackMessage(e) + "}");
        log.setLogLevel(LogLevel.ERROR.value);
        LogProducer.sendLog(log);
    }

    public static void logError(TSquid s, String message, Exception e) {
        SFLog log = new SFLog();
        log.from(s.getJobContext());
        log.setMessage("{message:" + message + ",EngineException:" + exceptionStackMessage(e) + "}");
        log.setLogLevel(LogLevel.ERROR.value);
        LogProducer.sendLog(log);
    }

    public static void logError(TSquidFlow squidFlow, Exception e) {
        SFLog log = new SFLog();
        log.from(squidFlow.getJobContext());
        log.setMessage("{EngineException:" + exceptionStackMessage(e) + "}");
        log.setLogLevel(LogLevel.ERROR.value);
        LogProducer.sendLog(log);
    }

    public static void logError(TSquidFlow squidFlow, String message) {
        SFLog log = new SFLog();
        log.from(squidFlow.getJobContext());
        log.setMessage(message);
        log.setLogLevel(LogLevel.ERROR.value);
        LogProducer.sendLog(log);
    }

}
