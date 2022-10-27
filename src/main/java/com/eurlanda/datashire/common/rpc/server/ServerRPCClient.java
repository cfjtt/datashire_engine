package com.eurlanda.datashire.common.rpc.server;

import com.eurlanda.datashire.common.rpc.IServerService;
import com.eurlanda.datashire.engine.service.RpcServerFactory;
import org.apache.avro.AvroRemoteException;

/**
 * Created by zhudebin on 14-5-17.
 */
public class ServerRPCClient {

    public static void main(String[] args) throws AvroRemoteException {
        squidStatus();
        System.exit(-1);
    }

    public static void squidStatus() throws AvroRemoteException {
        IServerService iServerService = RpcServerFactory.getServerService();
        iServerService.squidStatus("hello", 1, 1);
    }
}
