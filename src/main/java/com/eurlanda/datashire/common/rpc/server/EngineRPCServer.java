package com.eurlanda.datashire.common.rpc.server;

import com.eurlanda.datashire.common.rpc.IEngineService;
import com.eurlanda.datashire.common.rpc.impl.EngineServiceImpl;
import com.eurlanda.datashire.engine.entity.TSquidFlow;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.eurlanda.datashire.engine.util.ConfigurationUtil.getEngineRpcServerPort;

/**
 * Created by zhudebin on 14-5-17.
 */
public class EngineRPCServer {
    private static Log logger = LogFactory.getLog(EngineRPCServer.class);
    private static EngineRPCServer server;
    /**
     * taskId
     * TSquidFlow
     */
    public Map<String, TSquidFlow> squidFlowMap = Collections.synchronizedMap(new HashMap<String, TSquidFlow>());

    protected EngineRPCServer() {
        logger.info("RPC server starting .....");
        new NettyServer(new SpecificResponder(IEngineService.class, new EngineServiceImpl()), new InetSocketAddress(getEngineRpcServerPort()));
        logger.info("RPC server started");
    }

    public static EngineRPCServer getInstance() {
        if (server == null) {
            synchronized (EngineRPCServer.class) {
                if(server == null) {
                    server = new EngineRPCServer();
                    logger.info("server started:" + server.toString());
                }
            }
        }
        return server;
    }

    public TSquidFlow getSquidFlow(String taskId) {
        return squidFlowMap.get(taskId);
    }

    public void removeSquidFlow(String taskId) {
        squidFlowMap.remove(taskId);
    }

    public void addSquidFlow(TSquidFlow squidFlow) {
        if (squidFlow.isDebugModel()) {
            squidFlowMap.put(squidFlow.getTaskId(), squidFlow);
        }
    }


    public static void main(String[] args) throws Exception {
        launch();
    }

    /**
     * 启动接口
     */
    private static void launch() {
        EngineRPCServer.getInstance();
    }
}
