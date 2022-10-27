package com.eurlanda.datashire.engine.util;

import com.eurlanda.datashire.engine.entity.TSquid;
import com.eurlanda.datashire.engine.entity.TSquidFlow;
import com.eurlanda.datashire.engine.entity.TSquidType;
import com.eurlanda.datashire.engine.service.RpcServerFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * Created by zhudebin on 14-5-20.
 */
public class ServerRpcUtil {
    private static Log log = LogFactory.getLog(ServerRpcUtil.class);

//    private static int STATUS_START = 1;
//    private static int STATUS_WAIT = 2;
//    private static int STATUS_FAIL = 3;
//    private static int STATUS_SUCCESS = 4;

    public static final int STATUS_START = 0;
    private static final int STATUS_SUCCESS = 1;
    private static final int STATUS_FAIL = -1;
    public static final int STATUS_WAIT = 2;

    /**
     * 每一个squid都会推送一个开始状态
     *
     * @param tSquid
     * @param taskId
     */
    public static void sendSquidStartStatus(TSquid tSquid, String taskId, TSquidFlow tSquidFlow) {
        try {
            if(tSquidFlow.isDebugModel()) {
                RpcServerFactory.getServerService().squidStatus(taskId, tSquid.getSquidId(), STATUS_START);
                log.debug("推送squid 状态成功：" + taskId + "," + tSquid.getSquidId() + "," + STATUS_START);
            }
        } catch (IOException ioe) {
            log.error("调用后台server：sendSquidStartStatus 连接异常", ioe);
            EngineLogFactory.logError(tSquid, "调用后台server：sendSquidStartStatus 连接异常", ioe);
        } catch (Exception e) {
            log.error("调用后台server：sendSquidStartStatus 异常", e);
            EngineLogFactory.logError(tSquid, "调用后台server：sendSquidStartStatus 异常", e);
        }

    }

    /**
     * 通知connectionSquid运行完成
     * @param isDebug 是否为debug模式
     * @param tSquid extractSquid
     * @param connectionSquidId connectionSquid id
     */
    public static void sendConnectionSquidSuccess(boolean isDebug, TSquid tSquid, Integer connectionSquidId) {
        try {
            // debug模式下，connectionsquid 不为空
            if(isDebug && connectionSquidId != null) {
                RpcServerFactory.getServerService().squidStatus(tSquid.getJobContext().getTaskId(), connectionSquidId, STATUS_SUCCESS);
                log.debug("通知connectionSquid运行完成：" + tSquid.getJobContext().getTaskId() + "," + connectionSquidId + "," + STATUS_SUCCESS);
            }
        } catch (Exception e) {
            log.error("调用后台server：sendSquidFailStatus 异常", e);
            EngineLogFactory.logError(tSquid, "调用后台server：sendSquidFailStatus 异常", e);
        }
    }

    /**
     * 只有特殊的几个squid才能确定squid运行完成
     *
     * @param tSquid
     * @param taskId
     */
    public static void sendSquidSuccessStatus(TSquid tSquid, String taskId, TSquidFlow tSquidFlow) {
        if(!tSquidFlow.isDebugModel()) {
            return;
        }
        TSquidType tSquidType = tSquid.getType();

        switch (tSquidType) {
            case DEBUG_SQUID:
            case REPORT_SQUID:
            case DEST_ES_SQUID:
            case DEST_HDFS_SQUID:
            case DEST_IMPALA_SQUID:
                try {
                    RpcServerFactory.getServerService().squidStatus(taskId, tSquid.getSquidId(), STATUS_SUCCESS);
                    log.debug("推送squid 运行成功：" + taskId + "," + tSquid.getSquidId() + "," + STATUS_SUCCESS);
                } catch (Exception e) {
                    log.error("调用后台server：sendSquidStartStatus 异常", e);
                    EngineLogFactory.logError(tSquid, "调用后台server：sendSquidStartStatus 异常", e);
                }
            break;

        }
    }

    /**
     * 只要有一个squid异常，则整个squidflow 都失败了
     *
     * @param tSquid
     * @param taskId
     */
    public static void sendSquidFailStatus(TSquid tSquid, String taskId, TSquidFlow tSquidFlow) {
        try {
            if(tSquidFlow.isDebugModel()) {
                RpcServerFactory.getServerService().squidStatus(taskId, tSquid.getSquidId(), STATUS_FAIL);
                log.error("推送squid 运行失败：" + tSquidFlow.getTaskId() + "," + tSquid.getSquidId() + "," + STATUS_FAIL);
            }
        } catch (Exception e) {
            log.error("调用后台server：sendSquidFailStatus 异常", e);
            EngineLogFactory.logError(tSquid, "调用后台server：sendSquidFailStatus 异常", e);
        }
    }

    /**
     * squidflow 任务运行成功
     * @param tSquidFlow
     */
    public static void sendSquidFlowSuccess(TSquidFlow tSquidFlow) {
        try {
            if(tSquidFlow.isDebugModel()) {
                RpcServerFactory.getServerService().onTaskFinish(tSquidFlow.getTaskId(), STATUS_SUCCESS);
                log.debug("推送squidflow 运行成功：" + tSquidFlow.getTaskId() + "," + STATUS_SUCCESS);
            }
        } catch (Exception e) {
            log.error("调用后台server：sendSquidFlowSuccess 异常", e);
            EngineLogFactory.logError(tSquidFlow, e);
        }
    }

    /**
     * squidflow 任务运行失败
     * @param tSquidFlow
     */
    public static void sendSquidFlowFail(TSquidFlow tSquidFlow) {
        try {
            if(tSquidFlow.isDebugModel()) {
                RpcServerFactory.getServerService().onTaskFinish(tSquidFlow.getTaskId(), STATUS_FAIL);
            }
        } catch (Exception e) {
            log.error("调用后台server：sendSquidFlowFail 异常", e);
            EngineLogFactory.logError(tSquidFlow, e);
        }
    }


}
