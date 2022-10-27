package com.eurlanda.datashire.engine.util;

import com.eurlanda.datashire.engine.dao.SFJobModuleLogDao;
import com.eurlanda.datashire.engine.entity.SFJobModuleLog;
import com.eurlanda.datashire.engine.enumeration.JobModuleStatusEnum;

/**
 * Created by zhudebin on 14-5-6.
 */
public class JobLogUtil {

    /**
     * 更新squid flow job 模块的运行状态
     * @param taskId
     * @param squidId
     * @param jobModuleStatusEnum
     * @return
     */
    public static boolean updateJobModuleStatus(String taskId, int squidId, JobModuleStatusEnum jobModuleStatusEnum) {
        SFJobModuleLogDao sfJobModuleLogDao = ConstantUtil.getSFJobModuleLogDao();
        return sfJobModuleLogDao.updateStatus(taskId, squidId, jobModuleStatusEnum.value);
    }

    /**
     * 新增一条squid flow job 模块的运行状态
     * @param taskId
     * @param squidId
     * @param jobModuleStatusEnum
     * @return
     */
    public static boolean addJobModuleStatus(String taskId, int squidId, JobModuleStatusEnum jobModuleStatusEnum) {
        SFJobModuleLogDao sfJobModuleLogDao = ConstantUtil.getSFJobModuleLogDao();
        SFJobModuleLog sfJobModuleLog = new SFJobModuleLog();
        sfJobModuleLog.setTaskId(taskId);
        sfJobModuleLog.setSquidId(squidId);
        sfJobModuleLog.setStatus(jobModuleStatusEnum.value);
        return sfJobModuleLogDao.insert(sfJobModuleLog);
    }

}
