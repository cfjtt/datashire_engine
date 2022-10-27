package com.eurlanda.datashire.engine;

import com.eurlanda.datashire.common.rpc.server.EngineRPCServer;
import com.eurlanda.datashire.engine.dao.ScheduleDao;
import com.eurlanda.datashire.engine.entity.TSquidFlow;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.engine.util.time.TimerManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.CustomJavaSparkContext;

import static com.eurlanda.datashire.engine.util.ConfigurationUtil.*;

/**
 * Created by zhudebin on 14-6-10.
 * 测试使用的squidFlow启动类
 */
public class SquidFlowTestLauncher {

    private static Log log = LogFactory.getLog(SquidFlowTestLauncher.class);

    private static void init(boolean startRpc, boolean startSchedular,
                             boolean startLog) {
        if(startRpc) {
            // 启动 RMI
            startRPCServer();
        }

        if(startSchedular) {
            // 启动调度器。
            ScheduleDao sd = ConstantUtil.getScheduleDao();
            //ScheduleService.getInstance().addSchedules(sd.getDbTasks());
        }

        if(startLog) {
            //log
            TimerManager.startLogSchedule();
        }
    }

    /**
     * 启动 rpc 服务器
     */
    private static void startRPCServer() {
        EngineRPCServer.getInstance();
        log.info(" rpc server  started ............");
    }

    public static void launch(TSquidFlow tSquidFlow) {
        init(false,false,false);
        try {
            tSquidFlow.run(new CustomJavaSparkContext
                    (getSparkMasterUrl(), "SquidFlowLauncher", getSparkHomeDir(), getSparkJarLocation()));
        } catch (EngineException e) {
            e.printStackTrace();
        }

    }

    public static void launch(TSquidFlow tSquidFlow, boolean startRpc) {

    }

}
