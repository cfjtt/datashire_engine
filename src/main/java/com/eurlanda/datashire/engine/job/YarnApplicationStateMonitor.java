package com.eurlanda.datashire.engine.job;

import com.eurlanda.datashire.engine.dao.ApplicationStatusDao;
import com.eurlanda.datashire.engine.enumeration.JobStatusEnum;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

/**
 * Created by zhudebin on 16/3/11.
 */
public class YarnApplicationStateMonitor implements Runnable {

    private static Log log = LogFactory.getLog(YarnApplicationStateMonitor.class);
    public void run() {
        log.debug("=================== YarnApplicationStateMonitor.run() ====================");
        YarnClient client = null;
        try {
            // 会发生改变的状态
//            EnumSet<YarnApplicationState> yarnStateEnums = EnumSet.of(YarnApplicationState.ACCEPTED,
//                                                                      YarnApplicationState.FAILED,
//                                                                      YarnApplicationState.FINISHED,
//                                                                      YarnApplicationState.KILLED,
//                                                                      YarnApplicationState.NEW,
//                                                                      YarnApplicationState.NEW_SAVING,
//                                                                      YarnApplicationState.RUNNING,
//                                                                      YarnApplicationState.SUBMITTED);

            // 获取数据库中处于 running 中状态的提交记录
            ApplicationStatusDao statusDao = ConstantUtil.getApplicationStatusDao();
            List<Map<String, Object>> states = statusDao.getApplicationStatusByState("'"+JobStatusEnum.RUNNING.name()+"'"+",'"+FinalApplicationStatus.UNDEFINED.name()+"'");
            if(states == null || states.size() == 0) {
                return;
            }

            // 比较数据库中的状态和yarn集群状态,
            // 1. 如果yarn查询结构中存在,但是状态不一致,则修改数据库状态
            // 2. 如果yarn查询结果中不存在,标记为killed


            client = YarnClient.createYarnClient();
            client.init(new Configuration());
            client.start();
            for(Map<String, Object> map : states) {
                ApplicationReport report = client.getApplicationReport(genAppId(map.get("APPLICATION_ID").toString()));
                if(report == null) { // yarn上没有找到,情况(yarn 重启了)
                    log.info(String.format("没有找到%s作业", map.get("APPLICATION_ID").toString()));
                    statusDao.updateApplicationStatus(map.get("APPLICATION_ID").toString(), "KILLED");
                } else  if(report.getFinalApplicationStatus() != FinalApplicationStatus.UNDEFINED) {
                    log.info("========================updated===="+report.getApplicationId().toString()+"="+report.getFinalApplicationStatus().toString()+"======================");
                    statusDao.updateApplicationStatus(report.getApplicationId().toString(),report.getFinalApplicationStatus().toString());
                } else {
                    log.debug("作业正在运行中");
                }
            }

        } catch (Exception e) {
            log.error("更新流式作业状态异常", e);
        } finally {
            if(client != null) {
                client.stop();
            }
        }
    }

    /**
     * 将字符串解析成ApplicationId对象
     * @param appIdStr
     * @return
     */
    private ApplicationId genAppId(String appIdStr) {
//        application_1465960622896_0038
        String[] strs = appIdStr.split("_");
        long clusterTimestamp= Long.parseLong(strs[1]);
        int id = Integer.parseInt(strs[2]);
        return ApplicationId.newInstance(clusterTimestamp, id);
    }
}
