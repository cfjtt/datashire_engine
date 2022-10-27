package com.eurlanda.datashire.engine.dao;

import com.eurlanda.datashire.engine.entity.SFJobHistory;
import com.eurlanda.datashire.engine.enumeration.JobStatusEnum;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Repository;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by zhudebin on 14-5-6.
 */
@Repository
public class SFJobHistoryDao {

    private Logger logger = Logger.getLogger(SFJobHistoryDao.class);

    @Resource(name = "dataSource_sys")
    private DataSource dataSource_sys;

    /**
     * 插入一个 squidflow 运行记录
     * @param sfJobHistory
     * @return
     */
    public boolean insert(SFJobHistory sfJobHistory) {
        Connection conn = null;
        try {
            conn = dataSource_sys.getConnection();
            PreparedStatement pst = conn.prepareStatement(
                    "insert into DS_SYS_SF_JOB_HISTORY(task_id, repository_id, squid_flow_id, status, debug_squids, destination_squids, caller,job_id, create_time, update_time) " +
                            "values (?, ?, ?, ?, ?,?,?,?,now(),now())");
            pst.setString(1, sfJobHistory.getTaskId());
            pst.setInt(2, sfJobHistory.getRepositoryId());
            pst.setInt(3, sfJobHistory.getSquidFlowId());
            pst.setInt(4, sfJobHistory.getStatus());
            pst.setString(5, sfJobHistory.getDebugSquids());
            pst.setString(6, sfJobHistory.getDestinationSquids());
            pst.setString(7, sfJobHistory.getCaller());
            pst.setInt(8,sfJobHistory.getJobId());
            pst.executeUpdate();
        } catch (SQLException e) {
            logger.error("插入数据错误", e);
            return false;
        } finally {
            try {
                if(conn != null && (!conn.isClosed())) {
                    conn.close();
                }
            } catch (SQLException e) {
                logger.error(e);
            }
        }
        return true;
    }

    /**
     * 根据repositoryId, squidFlowId获取最新的 squidFlow运行记录
     * @param taskId
     * @return
     */
    public SFJobHistory getByTaskId(String taskId) {

        SFJobHistory sfJobHistory = null;
        Connection conn = null;
        try {
            conn = dataSource_sys.getConnection();
            PreparedStatement pst = conn.prepareStatement("select * from ds_sys_sf_job_history where task_id=?");
            pst.setString(1, taskId);

            ResultSet rs = pst.executeQuery();
            if(rs.next()) {
                sfJobHistory = new SFJobHistory();
                sfJobHistory.setTaskId(rs.getString("task_id"));
                sfJobHistory.setRepositoryId(rs.getInt("repository_id"));
                sfJobHistory.setSquidFlowId(rs.getInt("squid_flow_id"));
                sfJobHistory.setStatus(rs.getInt("status"));

            }
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if(conn != null && (!conn.isClosed())) {
                    conn.close();
                }
            } catch (SQLException e) {
                logger.error(e);
            }
        }
        return sfJobHistory;
    }

    /**
     * 根据repositoryId, squidFlowId获取最新的 squidFlow运行记录
     * @param repositoryId
     * @param squidFlowId
     * @return
     */
    public SFJobHistory getLast(int repositoryId, int squidFlowId) {

        SFJobHistory sfJobHistory = null;
        Connection conn = null;
        try {
            conn = dataSource_sys.getConnection();
            PreparedStatement pst = conn.prepareStatement("select * from ds_sys_sf_job_history where repository_id=? and squid_flow_id=? order by create_time limit 1");
            pst.setInt(1, repositoryId);
            pst.setInt(2, squidFlowId);

            ResultSet rs = pst.executeQuery();
            if(rs.next()) {
                sfJobHistory = new SFJobHistory();
                sfJobHistory.setTaskId(rs.getString("task_id"));
                sfJobHistory.setRepositoryId(rs.getInt("repository_id"));
                sfJobHistory.setSquidFlowId(rs.getInt("squid_flow_id"));
                sfJobHistory.setStatus(rs.getInt("status"));

            }
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if(conn != null && (!conn.isClosed())) {
                    conn.close();
                }
            } catch (SQLException e) {
                logger.error(e);
            }
        }
        return sfJobHistory;
    }

    /**
     * 更新job status
     * @param taskId  作业id
     * @param squidFlowId 作业squid flow id
     * @param status
     * @return
     */
    public boolean updateStatus(String taskId, int squidFlowId, JobStatusEnum status) {
        Connection conn = null;
        try {
            conn = dataSource_sys.getConnection();
            PreparedStatement pst = conn.prepareStatement(
                    "update DS_SYS_SF_JOB_HISTORY set status=?,update_time=now() where task_id=? and squid_flow_id=? ");
            pst.setInt(1, status.value);
            pst.setString(2, taskId);
            pst.setInt(3, squidFlowId);
            pst.executeUpdate();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("插入SFJobModuleLog失败", e);
            return false;
        } finally {
            try {
                if(conn != null && (!conn.isClosed())) {
                    conn.close();
                }
            } catch (SQLException e) {
                logger.error(e);
            }
        }
        return true;
    }
}
