package com.eurlanda.datashire.engine.schedule;

import com.eurlanda.datashire.engine.service.EngineService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * spark任务。
 * 
 * @date 2014-4-30
 * @author jiwei.zhang
 * 
 */
public class SquidFlowTask extends SparkTask {
    private Logger logger = LoggerFactory.getLogger(SquidFlowTask.class);

	public SquidFlowTask(ScheduleInfo job) {
		super(job);
	}

	@Override
	public void run() {
		int sfId = this.getJob().getSquidFlowId();
		int repId = this.getJob().getRepositoryId();
		String taskId = null;
		try {
			taskId = EngineService.launchJob(sfId, repId, this.getJob().getId(), this.getJob().getEmails());
			/**
			 * 添加调度记录。
			 * 
			 * this.jdbcTemplate.execute(arg0);
			 */
			logger.info("任务已经成功启动，id:{0}", sfId);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
