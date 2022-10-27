package com.eurlanda.datashire.engine.dao;

import com.eurlanda.datashire.engine.entity.SFLog;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.engine.util.UUIDUtil;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by Juntao.Zhang on 2014/5/28.
 */
public class SFLogDaoTest {
    SFLogDao sfLogDao;

    @Before
    public void setUpTest() throws Exception {
        sfLogDao = new SFLogDao(ConstantUtil.getSysDataSource());
    }

    @Test
    public void testInsert() throws SQLException {
        org.apache.hadoop.io.Writable a;
        org.apache.hadoop.mapred.JobConfigurable jc;
        com.codahale.metrics.Metric m;
        org.apache.hadoop.security.UserGroupInformation b;
        org.apache.hadoop.util.PlatformName p;

        SFLog log = new SFLog();
        log.setJobId(1);
        log.setLogLevel(2);
        log.setMessage("message");
        log.setProjectId(3);
        log.setProjectName("pn-3");
        log.setRepositoryId(4);
        log.setRepositoryName("r-4");
        log.setSquidFlowId(5);
        log.setSquidFlowName("sf-5");
        log.setSquidId(6);
        log.setSquidName("s-6");
        log.setTSquidId("s6");
        log.setTSquidType(7);
        log.setTaskId(UUIDUtil.genUUID());
        sfLogDao.insert(log);
    }

    @Test
    public void init() throws SQLException {
        sfLogDao.initTable();
    }

    @Test
    public void list() {
        try {
            Connection conn = ConstantUtil.getSysDataSource().getConnection();
            ResultSet rs = conn.createStatement().executeQuery("SELECT " +
                            "id,job_id,repository_id,repository_name,project_id,project_name," +
                            "squid_flow_id,squid_flow_name,squid_id,squid_name,task_id,message," +
                            "log_level,create_time,tsquid_id,tsquid_type" +
                            " FROM DS_SYS.SF_LOG"
            );
            System.out.println("===========================================================");
            while (rs.next()) {
                System.out.println(
                        rs.getInt(1) + "," + rs.getInt(2) + "," + rs.getString(4) + "," + rs.getInt(3) + "," + rs.getString(6) + "," + rs.getInt(5) + "," + rs.getString(8) + "," +
                                rs.getInt(7) + "," + rs.getInt(9) + "," + rs.getString(10) + "," + rs.getString(11) + "," + rs.getString(12) + "," +
                                rs.getInt(13) + "," + rs.getDate(14) + "," + rs.getString(15) + "," + rs.getInt(16)
                );
            }
            System.out.println("===========================================================");
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void clearTable() throws SQLException {
        Connection conn = ConstantUtil.getSysDataSource().getConnection();
        conn.createStatement().executeUpdate("DELETE FROM DS_SYS.SF_LOG");
        conn.commit();
        conn.close();
    }


}
