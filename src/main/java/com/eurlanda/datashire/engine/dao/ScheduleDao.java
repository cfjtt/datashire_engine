package com.eurlanda.datashire.engine.dao;

import com.eurlanda.datashire.adapter.HyperSQLManager;
import com.eurlanda.datashire.server.model.ScheduleJob;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ScheduleDao {
	private JdbcTemplate jdbcTemplate;
	public ScheduleDao(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	/**
	 * 
	 CREATE TABLE DS_SYS_JOB_SCHEDULE ( ID INTEGER NOT NULL, TEAM_ID INTEGER,
	 * REPOSITORY_ID INTEGER, PROJECT_ID INTEGER, PROJECT_NAME VARCHAR(100),
	 * SQUID_FLOW_ID INTEGER, SQUID_FLOW_NAME VARCHAR(100), SCHEDULE_TYPE
	 * VARCHAR(50), SCHEDULE_BEGIN_DATE TIMESTAMP, SCHEDULE_END_DATE TIMESTAMP,
	 * SCHEDULE_VALID INTEGER, DAY_DELY INTEGER, DAY_RUN_COUNT INTEGER,
	 * DAY_BEGIN_DATE TIME, DAY_END_DATE TIME, DAY_RUN_DELY INTEGER, WEEK_DAY
	 * INTEGER, WEEK_BEGIN_DATE TIME, MONTH_DAY INTEGER, MONTH_BEGIN_DATE TIME,
	 * LAST_SCHEDULED_DATE TIMESTAMP, CREATION_DATE TIMESTAMP DEFAULT
	 * CURRENT_TIMESTAMP, STATUS CHAR(1) );
	 * 
	 * @return
	 */
	public List<ScheduleJob> getJobScheduleList() {
		String sql ="select job.*,dsp.id as repository_id from ds_sys_schedule_job job,ds_squid_flow dsf,ds_project dp,ds_sys_repository dsp where job.job_status=1 and job.squid_flow_id = dsf.id and dsf.project_id = dp.id and dp.repository_id = dsp.id ";
		List<ScheduleJob> jobs = new ArrayList<>();
		try {
			List<Map<String,Object>> jobsMap = HyperSQLManager.query2List(jdbcTemplate.getDataSource().getConnection(),true,sql,null);
			for(Map<String,Object> jobMap : jobsMap){
				ScheduleJob job = new ScheduleJob();
				job.setId((Integer) jobMap.get("ID"));
				job.setSquid_flow_id((Integer) jobMap.get("SQUID_FLOW_ID"));
				job.setSchedule_type((Integer)jobMap.get("SCHEDULE_TYPE"));
				job.setCron_expression(jobMap.get("CRON_EXPRESSION")+"");
				job.setEnable_email((Integer) jobMap.get("ENABLE_EMAIL"));
				job.setEmail_address(jobMap.get("EMAIL_ADDRESS")+"");
				job.setJob_status((Integer) jobMap.get("JOB_STATUS"));
				job.setRepository_id((Integer) jobMap.get("REPOSITORY_ID"));
				jobs.add(job);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return jobs;
	}

	/*
	 * public List<SquidSchedule> getSquidScheduleList() { List<Map<String,
	 * Object>> list = jdbcTemplate.queryForList(
	 * "select * from DS_SYS_JOB_SCHEDULE where schedule_valid=1");
	 * List<SquidSchedule> jobs = new ArrayList<>(); SquidSchedule js = new
	 * SquidSchedule(); for (Map<String, Object> params : list) { try {
	 * BeanUtils.populate(js, params); } catch (IllegalAccessException e) {
	 * e.printStackTrace(); } catch (InvocationTargetException e) {
	 * e.printStackTrace(); } jobs.add(js); } return jobs; }
	 */

	/**
	 * 开始数据库任务。
	 */
	/*public List<SparkTask> getDbTasks() {
		List<SparkTask> infoList = new ArrayList<SparkTask>();
		for (JobSchedule js : this.getJobScheduleList()) {
			try{
			    infoList.add(this.job2Task(js));
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		return infoList;
	}*/

	/*public SparkTask job2Task(JobSchedule js) {
		ScheduleInfo info = new ScheduleInfo();
		if (StringUtils.isNotEmpty(js.getDay_begin_date())) {
			info.setDayBeginDate(DateTimeUtils.parserDateTime(js.getDay_begin_date(), "hh:mm:ss"));
		}
		info.setDayDely(js.getDay_dely());
		if (StringUtils.isNotEmpty(js.getDay_end_date())) {
			info.setDayEndDate(DateTimeUtils.parserDateTime(js.getDay_end_date(), "hh:mm:ss"));
		}
		Time onceBeginTime = Time.valueOf(js.getDay_once_off_time());
		info.setDayOnceBeginDate(onceBeginTime);
		info.setDayRunCount(js.getDay_run_count());
		info.setDayRunDely(js.getDay_run_dely());
		info.setId(js.getId());
		if (StringUtils.isNotEmpty(js.getLast_scheduled_date())) {
			info.setLastScheduledDate(DateTimeUtils.parserDateTime(js.getLast_scheduled_date(), "yyyy-MM-dd hh:mm:ss"));
		}

		if (StringUtils.isNotEmpty(js.getMonth_begin_date())) {
			info.setMonthBeginDate(DateTimeUtils.parserDateTime(js.getMonth_begin_date(), "hh:mm:ss"));
		}
		info.setMonthDay(js.getMonth_day());
		info.setRepositoryId(js.getRepository_id());
		if (StringUtils.isNotEmpty(js.getSchedule_begin_date())) {
			info.setScheduleBeginDate(DateTimeUtils.parserDateTime(js.getSchedule_begin_date(), "yyyy-MM-dd hh:mm:ss"));
		}
		if (StringUtils.isNotEmpty(js.getSchedule_end_date())) {
			info.setScheduleEndDate(DateTimeUtils.parserDateTime(js.getSchedule_end_date(), "yyyy-MM-dd hh:mm:ss"));
		}
		info.setScheduleType(js.getSchedule_type());
		info.setScheduleValid(js.getSchedule_valid() == 1 ? true : false);
		info.setSquidFlowId(js.getSquid_flow_id());
		if (StringUtils.isNotEmpty(js.getWeek_begin_date())) {
			info.setWeekBeginDate(DateTimeUtils.parserDateTime(js.getWeek_begin_date(), "hh:mm:ss"));
		}
		info.setWeekDay(js.getWeek_day());
		info.setSquidId(js.getSquid_id());
        // 是否允许发送邮件
        if(js.getEnable_email_sending() != 0) {
            info.setEmails(js.getEmail_address());
        }
		SparkTask task=null;
		if(js.getSquid_id()>0){
			task= new SquidTask(info);
		}else{
			task = new SquidFlowTask(info);
		}
		return task;
	}*/

	public ScheduleJob getScheduleJobById(int id) {
		String sql ="select job.*,dsp.id as repository_id from ds_sys_schedule_job job,ds_squid_flow dsf,ds_project dp,ds_sys_repository dsp where  job.squid_flow_id = dsf.id and dsf.project_id = dp.id and dp.repository_id = dsp.id and job.id="+id;
		ScheduleJob job = null ;
		try {
			List<Map<String,Object>> jobsMap = HyperSQLManager.query2List(jdbcTemplate.getDataSource().getConnection(),true,sql,null);
			if(jobsMap!=null && jobsMap.size()>0){
				Map<String,Object> jobMap  = jobsMap.get(0);
				job = new ScheduleJob();
				job.setId((Integer) jobMap.get("ID"));
				job.setSquid_flow_id((Integer) jobMap.get("SQUID_FLOW_ID"));
				job.setSchedule_type((Integer)jobMap.get("SCHEDULE_TYPE"));
				job.setCron_expression(jobMap.get("CRON_EXPRESSION")+"");
				job.setEnable_email((Integer) jobMap.get("ENABLE_EMAIL"));
				job.setEmail_address(jobMap.get("EMAIL_ADDRESS")+"");
				job.setJob_status((Integer) jobMap.get("JOB_STATUS"));
				job.setName(jobMap.get("NAME")+"");
				job.setRepository_id((Integer) jobMap.get("REPOSITORY_ID"));
			} else {
				throw new RuntimeException("未找到任务调度定义"+id+"，可能是待调度不可运行。");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return job;
	}

	/*protected Map<String, Object> createColumnMap(int columnCount) {
		return new LinkedCaseInsensitiveMap<Object>(columnCount);
	}

	protected String getColumnKey(String columnName) {
		return columnName;
	}

	protected Object getColumnValue(ResultSet rs, int index) throws SQLException {
		return JdbcUtils.getResultSetValue(rs, index);
	}*/
}
