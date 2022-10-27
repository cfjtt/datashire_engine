package com.eurlanda.datashire.engine.dao;

import com.eurlanda.datashire.engine.entity.SFJobModuleLog;
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
public class SFJobModuleLogDao {

    private Logger logger = Logger.getLogger(SFJobModuleLogDao.class);

    @Resource(name = "dataSource_sys")
    private DataSource dataSource_sys;

    /**
     * 保存squidflow job 模块状态，如果该状态存在则更新，否则新增插入
     * @param taskId
     * @param squidId
     * @param status
     * @return
     */
    public boolean replaceStatus(String taskId, int squidId, int status) {
        Connection conn = null;
        try {
            conn = dataSource_sys.getConnection();
            // 查询该模块状态是否存在
            PreparedStatement selectPst = conn.prepareStatement("select * from DS_SYS_SF_JOB_MODULE_LOG where task_id=? and squid_id=?");
            selectPst.setString(1, taskId);
            selectPst.setInt(2, squidId);
            ResultSet rs = selectPst.executeQuery();
            if(rs.next()) {    // 存在  -> 更新
                PreparedStatement pst = conn.prepareStatement(
                        "update DS_SYS_SF_JOB_MODULE_LOG set status=?, update_time=now() where task_id=? and squid_id=?");
                pst.setInt(1, status);
                pst.setString(2, taskId);
                pst.setInt(3, squidId);
                pst.executeUpdate();
            } else {
                // 不存在 -> 插入
                PreparedStatement pst = conn.prepareStatement(
                        "insert into DS_SYS_SF_JOB_MODULE_LOG(task_id, squid_id, status, create_time, update_time) " +
                                "values(?, ?, ?, now(), now())");
                pst.setString(1, taskId);
                pst.setInt(2, squidId);
                pst.setInt(3, status);
                pst.executeUpdate();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("插入SFJobModuleLog失败", e);
            return false;
        } finally {
            if(conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    logger.error("关闭连接失败", e);
                }
            }
        }
        return true;

    }

    /**
     * 插入一个squidflow job 模块状态
     * @param sfJobModuleLog
     * @return
     */
    public boolean insert(SFJobModuleLog sfJobModuleLog) {
        Connection conn = null;
        try {
            conn = dataSource_sys.getConnection();
            PreparedStatement pst = conn.prepareStatement(
                    "insert into DS_SYS_SF_JOB_MODULE_LOG(task_id, squid_id, status, create_time, update_time) " +
                            "values(?, ?, ?, now(), now())");
            pst.setString(1, sfJobModuleLog.getTaskId());
            pst.setInt(2, sfJobModuleLog.getSquidId());
            pst.setInt(3, sfJobModuleLog.getStatus());
            pst.executeUpdate();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("插入SFJobModuleLog失败", e);
            return false;
        } finally {
            if(conn != null) {
                try {
                	if(!conn.isClosed())
                		conn.close();
                } catch (SQLException e) {
                    logger.error("关闭连接失败", e);
                }
            }
        }
        return true;
    }

    /**
     * 更新
     * @param taskId
     * @param squidId
     * @param status
     * @return
     */
    public boolean updateStatus(String taskId, int squidId, int status) {
        Connection conn = null;
        try {
            conn = dataSource_sys.getConnection();
            PreparedStatement pst = conn.prepareStatement(
                    "update DS_SYS_SF_JOB_MODULE_LOG set status=?,update_time=now() where task_id=? and squid_id=? ");
            pst.setInt(1, status);
            pst.setString(2, taskId);
            pst.setInt(3, squidId);
            pst.executeUpdate();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("插入SFJobModuleLog失败", e);
            return false;
        } finally {
            if(conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    logger.error("关闭连接失败", e);
                }
            }
        }
        return true;
    }
}
