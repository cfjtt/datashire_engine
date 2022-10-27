package com.eurlanda.datashire.engine;

import com.eurlanda.datashire.common.rpc.server.EngineRPCServer;
import com.eurlanda.datashire.common.webService.DataShireApiImpl;
import com.eurlanda.datashire.engine.job.YarnApplicationStateMonitor;
import com.eurlanda.datashire.engine.schedule.ScheduleJobManager;
import com.eurlanda.datashire.engine.service.SquidFlowLauncher;
import com.eurlanda.datashire.engine.util.ConfigurationUtil;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.hive.thriftserver.ThriftServer2Util;

import javax.xml.ws.Endpoint;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

/**
 * 引擎启动入口
 * Created by zhudebin on 14-4-26.
 */
public class EngineServer {

    private static Log log = LogFactory.getLog(EngineServer.class);

    public static void main(String[] args) {
        ConstantUtil.init();

        // 设置为批处理引擎
        SquidFlowLauncher.setBatchJob();
        // 启动 RPC
        startRPCServer();

        SquidFlowLauncher.initSparkContext();
        //启动调度
        if(ConfigurationUtil.isStartSchedule()) {
            startSchedular();
        }

        if(!SquidFlowLauncher.local) {
            // administrator。

            Endpoint.publish("http://"+ConfigurationUtil.getWebServiceIp()+":"+ ConfigurationUtil.getWebServicePort()+"/"+ConfigurationUtil.getWebServiceUrl(), new DataShireApiImpl());

            //log
//            TimerManager.startLogSchedule();
            yarnStateSynchronized();
        }


        if(ConfigurationUtil.startThriftServer()) {
            ThriftServer2Util.startThriftServer();
        }

        keepAlive();
    }

    private static void keepAlive() {
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 启动 rpc 服务器
     */
    private static void startRPCServer() {
        EngineRPCServer.getInstance();
        log.info("--------------------- rpc server  started ---------------------");
    }

    /**
     * 启动调度
     */
    private static void startSchedular() {
        // 启动调度器。
        ScheduleJobManager manager = ScheduleJobManager.getInstance();
        manager.startSchedule();
        log.info("作业调度 启动成功..........");
    }

    /**
     * 启动任务状态同步
     */
    private static void yarnStateSynchronized(){
        ScheduledExecutorService service =
                newSingleThreadScheduledExecutor();
        YarnApplicationStateMonitor yarnApplicationStateMonitor= new YarnApplicationStateMonitor();
        // 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
        service.scheduleAtFixedRate(yarnApplicationStateMonitor, 0, 5, TimeUnit.SECONDS);
        log.info("任务状态同步 启动成功..........");

    }
}
