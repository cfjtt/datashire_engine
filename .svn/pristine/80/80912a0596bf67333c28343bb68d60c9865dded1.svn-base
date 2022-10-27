package com.eurlanda.datashire.engine.dao;

import com.eurlanda.datashire.engine.entity.SFJobModuleLog;
import com.eurlanda.datashire.engine.enumeration.JobModuleStatusEnum;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.engine.util.UUIDUtil;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by zhudebin on 14-5-6.
 */
public class SFJobModuleLogDaoTest {
    SFJobModuleLogDao sfJobModuleLogDao = null;

    @Before
    public void setUpTest() throws Exception {
        sfJobModuleLogDao = ConstantUtil.getSFJobModuleLogDao();
    }

    @Test
    public void testInsert() {
        SFJobModuleLog sfJobModuleLog = new SFJobModuleLog();
        sfJobModuleLog.setTaskId(UUIDUtil.genUUID());
//        sfJobModuleLog.setTaskId("1d7f3c42-fbc0-4a01-8a72-164620e15499");
        sfJobModuleLog.setSquidId(1);
        sfJobModuleLog.setStatus(JobModuleStatusEnum.STAGE01.value);

        sfJobModuleLogDao.insert(sfJobModuleLog);

        sfJobModuleLogDao.updateStatus(sfJobModuleLog.getTaskId(), 1, JobModuleStatusEnum.SUCCESS.value);


    }

    @Test
    public void testUpdate() {
        sfJobModuleLogDao.updateStatus("1d7f3c42-fbc0-4a01-8a72-164620e15499", 1, JobModuleStatusEnum.SUCCESS.value);
    }

    @Test
    public void testReplace() {
        sfJobModuleLogDao.replaceStatus("1d7f3c42-fbc0-4a01-8a72-164620e15498", 1, JobModuleStatusEnum.STAGE02.value);
    }

}
