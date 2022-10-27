package com.eurlanda.datashire.engine.dao;

import com.eurlanda.ClientCaseTest;
import com.eurlanda.datashire.engine.entity.SFJobHistory;
import com.eurlanda.datashire.engine.enumeration.JobStatusEnum;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.engine.util.UUIDUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by zhudebin on 14-5-6.
 */
public class FSJobHistoryDaoTest extends ClientCaseTest {

    SFJobHistoryDao sfJobHistoryDao = null;

    @Before
    public void setUpTest() throws Exception {
        sfJobHistoryDao = ConstantUtil.getSFJobHistoryDao();
    }

    @Test
    public void testInsert() {
        SFJobHistory sfJobHistory = new SFJobHistory();
        sfJobHistory.setTaskId(UUIDUtil.genUUID());
        sfJobHistory.setRepositoryId(1);
        sfJobHistory.setSquidFlowId(1);
        sfJobHistory.setStatus(JobStatusEnum.RUNNING.value);
        sfJobHistoryDao.insert(sfJobHistory);

        SFJobHistory history = sfJobHistoryDao.getLast(1, 1);
        sfJobHistoryDao.updateStatus(history.getTaskId(), 1, JobStatusEnum.SUCCESS);
        history = sfJobHistoryDao.getLast(1, 1);
        Assert.assertEquals(history.getStatus(), JobStatusEnum.SUCCESS.value);
    }

    @Test
    public void testGetLast() {
        SFJobHistory sfJobHistory = sfJobHistoryDao.getLast(1, 1);
        Assert.assertNotNull(sfJobHistory);
    }


}
