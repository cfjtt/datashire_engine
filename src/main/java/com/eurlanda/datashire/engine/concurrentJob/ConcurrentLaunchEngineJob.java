package com.eurlanda.datashire.engine.concurrentJob;

import com.eurlanda.datashire.common.rpc.IEngineService;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by Eurlanda on 2017/7/10.
 */
public class ConcurrentLaunchEngineJob implements Runnable{

    private static IEngineService service;

    static  {
        synchronized (ConcurrentLaunchEngineJob.class) {
            if(service == null) {
                NettyTransceiver client = null;
                try {
                    client = new NettyTransceiver(new InetSocketAddress("192.168.137.101", 11099));
                    service = SpecificRequestor.getClient(IEngineService.class, client);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private int squidFlowId;
    private int repositoryId;
    private String params;

    public ConcurrentLaunchEngineJob(int squidFlowId, int repositoryId, String params) {
        this.squidFlowId = squidFlowId;
        this.repositoryId = repositoryId;
        this.params = params;
    }

    @Override
    public void run() {
        try {
            CharSequence taskId = service.launchEngineJob(squidFlowId,repositoryId,params,"");
            System.out.println("---- squidflowId ----" + squidFlowId + " -- taskId:" + taskId.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getSquidFlowId() {
        return squidFlowId;
    }

    public void setSquidFlowId(int squidFlowId) {
        this.squidFlowId = squidFlowId;
    }

    public int getRepositoryId() {
        return repositoryId;
    }

    public void setRepositoryId(int repositoryId) {
        this.repositoryId = repositoryId;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }
}
