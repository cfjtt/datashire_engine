package com.eurlanda.datashire.engine.service;

import com.eurlanda.datashire.common.rpc.ICrawlerService;
import com.eurlanda.datashire.common.rpc.IEngineService;
import com.eurlanda.datashire.common.rpc.IReportService;
import com.eurlanda.datashire.common.rpc.IServerService;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

import static com.eurlanda.datashire.engine.util.ConfigurationUtil.*;

/**
 * Created by zhudebin on 14-5-17.
 */
public class RpcServerFactory {

    private static Log log = LogFactory.getLog(RpcServerFactory.class);

    private static IReportService iReportService;
    private static IEngineService iEngineService;
    private static IServerService iServerService;
    private static ICrawlerService crawlerService;

    
    public static IReportService getReportService() {
        if (iReportService == null) {
            synchronized (RpcServerFactory.class) {
                try {
                    if (iReportService == null) {
                        NettyTransceiver client = new NettyTransceiver(new InetSocketAddress(getReportRpcServerIp(), getReportRpcServerPort()));
                        iReportService = SpecificRequestor.getClient(IReportService.class, client);
                    }
                } catch (IOException e) {
                    log.error("报表RPC连接异常", e);
                }
            }
        }
        return iReportService;
    }

    public static IEngineService getEngineService() {
        if (iEngineService == null) {
            synchronized (RpcServerFactory.class) {
                try {
                    if (iEngineService == null) {
                        iEngineService = SpecificRequestor.getClient(
                                IEngineService.class,
                                new NettyTransceiver(new InetSocketAddress(getEngineRpcServerIp(), getEngineRpcServerPort()))
                        );
                    }
                } catch (IOException e) {
                    log.error("引擎RPC连接异常", e);
                }
            }
        }
        return iEngineService;
    }

    public static IServerService getServerService() {
        if (iServerService == null) {
            synchronized (RpcServerFactory.class) {
                try {
                    if (iServerService == null) {
                        NettyTransceiver client = new NettyTransceiver(new InetSocketAddress(getServerRpcServerIp(), getServerRpcServerPort()));
                        iServerService = SpecificRequestor.getClient(IServerService.class, client);
                    }
                } catch (IOException e) {
                    log.error("后台server RPC连接异常", e);
                }
            }
        }
        return iServerService;
    }
    public static ICrawlerService getCrawlerService() {
        if (crawlerService == null) {
            synchronized (RpcServerFactory.class) {
                try {
                    if (crawlerService == null) {
                        NettyTransceiver client = new NettyTransceiver(new InetSocketAddress(
                                getCrawlerRpcServerIp(),getCrawlerRpcServerPort()));
                        crawlerService = SpecificRequestor.getClient(ICrawlerService.class, client);
                    }
                } catch (IOException e) {
                    log.error("爬虫RPC连接异常", e);
                }
            }
        }
        return crawlerService;
    }
}
