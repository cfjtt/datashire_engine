package com.eurlanda.datashire.engine.schedule;

import com.eurlanda.datashire.engine.dao.ScheduleDao;
import com.eurlanda.datashire.engine.util.ConstantUtil;
import com.eurlanda.datashire.server.model.ScheduleJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

/**
 * 线程调度service.实现了基于天、星期、月份的自由调度功能。
 * 
 * @date 2014-4-30
 * @author jiwei.zhang
 * 
 */
public class ScheduleService{
	private static Logger log = LoggerFactory.getLogger(ScheduleService.class);
	//private ExecutorService pool;
	private JdbcTemplate jdbcTemplate = new JdbcTemplate(ConstantUtil.getSysDataSource());
	private static ScheduleService instance;

	public static ScheduleService getInstance() {
		if (instance == null) {
			instance = new ScheduleService();
		}
		return instance;
	}

	/**
	 * 构造一个调度器，并启动调度线程。
	 */
	private ScheduleService() {
		//pool = Executors.newCachedThreadPool();
		instance=this;
	}

	/**
	 * 获取所有的可用的调度
	 * @return
	 */
	public List<ScheduleJob> getScheduleJobs(){
		ScheduleDao dao = new ScheduleDao(jdbcTemplate);
		List<ScheduleJob> jobs = dao.getJobScheduleList();
		System.out.println("获取到了job的大小:"+jobs.size());
		return jobs;
	}

	/**
	 * 根据id获取调度
	 * @param jobId
	 * @return
	 */
	public ScheduleJob getScheduleJobById(int jobId){
		ScheduleDao dao = new ScheduleDao(jdbcTemplate);
		return dao.getScheduleJobById(jobId);
	}
	/**
	 * 检查天任务。
	 * 
	 * @date 2014-4-30
	 * @author jiwei.zhang
	 * @param task
	 */
	/*private void checkDayTask(SparkTask task) {
		ScheduleInfo js = task.getJob();

		Date nowDate = new Date();
		long lastDate = js.getLastScheduledDate() == null ? 0 : js.getLastScheduledDate().getTime();
		Calendar cNow = Calendar.getInstance();
		cNow.setTime(nowDate);
		Calendar cLast = Calendar.getInstance();
		cLast.setTimeInMillis(lastDate);
		//判断当前时间和最后一次运行时间，相隔天数
		int nowDely = cNow.get(Calendar.DAY_OF_MONTH) - cLast.get(Calendar.DAY_OF_MONTH) ;
		// todo: 单日任务还没判断。增加了dayOnceBeginDate;
		if (nowDely >= js.getDayDely()) { // 检查运行天数间隔

			if (js.getDayRunCount() == 1) { // 一天运行一次
				//判断当前时间的时分秒是否到了指定的运行时间
				long executeTime = js.getDayOnceBeginDate().getTime();
				long nowTime = getTimeOfDate(js.getDayOnceBeginDate());
				if(!isSameDay(cNow.getTime(),cLast.getTime()) && nowTime>=executeTime){
					executeTask(task);
				}
			} else { // 一天运行多次
				// 转换时间
				long nowTime = getTimeOfDate(nowDate);
				long dayEndTime = getTimeOfDate(js.getDayEndDate());
				long dayBeginTime = getTimeOfDate(js.getDayBeginDate());
				
				// 网页squid和微博squid不校验开始结束日期。
				boolean isSquid = task.getJob().getSquidId()>0;
				if ( isSquid || (nowTime >= dayBeginTime && nowTime <= dayEndTime)) { // 检查运行时间。
					long dayRunDely = js.getDayRunDely() * 60 * 1000; // 换成毫秒
					if ((nowDate.getTime() - lastDate) >= dayRunDely) { // *2
						executeTask(task); // 任务已经超过一天，运行任务。
					}

				}
			}

		}

	}*/

	/**
	 * 检查哪些任务需要运行。
	 * 
	 * @date 2014-4-30
	 * @author jiwei.zhang
	 */
	/*public void checkTask(SparkTask task) {
		// 开始检查
		ScheduleInfo js = task.getJob();

		Date nowDate = new Date();

		// 检查是否是squid调度，如果是squid,要检查对应的squidFlow有没有在调试，只有SquidFlow调度时它才可以调度。
		*//*if (js.getSquidId() != 0) { // squid 调度
			boolean isSquidFlowStarted = false;
			for (SparkTask x : this.tasks) {
				if (js.getSquidFlowId() == x.getJob().getSquidFlowId() && x.getJob().getSquidId() == 0) {
					isSquidFlowStarted = true;
					break;
				}
			}
			if (!isSquidFlowStarted) {
				log.debug("该squid[{}]对应的squidFlow[{}]未启动，暂时停止本squid的调度",js.getSquidId(),js.getSquidFlowId());
				return;
		
			}
		}*//*

		if (js.isScheduleValid()) { // 检查是否要运行。
			long now = nowDate.getTime();
			if (js.getSquidId() != 0) {
				// 检查运行类型，是天是星期，还是月
				if ("1".equals(js.getScheduleType())) {
					checkDayTask(task);
				} else if ("2".equals(js.getScheduleType())) {
					checkWeekTask(task);
				} else if ("3".equals(js.getScheduleType())) {
					checkMonthTask(task);
				}
			} else {
				if (js.getScheduleBeginDate() != null
						&& js.getScheduleBeginDate().getTime() < now
						&& js.getScheduleEndDate().getTime() > now) { // 运行日期检查。
					// 检查运行类型，是天是星期，还是月
					if ("1".equals(js.getScheduleType())) {
						checkDayTask(task);
					} else if ("2".equals(js.getScheduleType())) {
						checkWeekTask(task);
					} else if ("3".equals(js.getScheduleType())) {
						checkMonthTask(task);
					}
				}
			}
		}
	}*/

	/**
	 * 检查月任务
	 * 
	 * @date 2014-4-30
	 * @author jiwei.zhang
	 * @param task
	 */
	/*private void checkMonthTask(SparkTask task) {
		ScheduleInfo js = task.getJob();

		Date nowDate = new Date();
		long lastDate = js.getLastScheduledDate() == null ? 0 : js.getLastScheduledDate().getTime();
		Calendar cNow = Calendar.getInstance();
		cNow.setTime(nowDate);
		Calendar cLast = Calendar.getInstance();
		cLast.setTimeInMillis(lastDate);

		int nowMonthDay = cNow.get(Calendar.DAY_OF_MONTH);

		if (nowMonthDay == js.getMonthDay()) {

			long nowTime = getTimeOfDate(nowDate);
			long dayBeginTime = getTimeOfDate(js.getWeekBeginDate());
			int lastMonthDay = cLast.get(Calendar.DAY_OF_MONTH);

			if (!isSameDay(cNow.getTime(),cLast.getTime()) && nowTime >= dayBeginTime) { // 检查运行时间。
				executeTask(task);
			}

		}

	}*/

	/**
	 * 检查周任务。
	 * 
	 * @date 2014-4-30
	 * @author jiwei.zhang
	 * @param task
	 */
	/*private void checkWeekTask(SparkTask task) {
		ScheduleInfo js = task.getJob();

		Date nowDate = new Date();
		long lastDate = js.getLastScheduledDate() == null ? 0 : js.getLastScheduledDate().getTime();
		//当前时间
		Calendar cNow = Calendar.getInstance();
		cNow.setTime(nowDate);
		//最后一次运行时间
		Calendar cLast = Calendar.getInstance();
		cLast.setTimeInMillis(lastDate);

		int nowWeekDay = cNow.get(Calendar.DAY_OF_WEEK); // 今天星期几

		////检查当前时间和最后一次运行时间不是同一天，并且当前时间符合要求
		if (!isSameDay(nowDate, cLast.getTime()) && nowWeekDay == js.getWeekDay()) { // 检查星期。

			long nowTime = getTimeOfDate(nowDate);
			long dayBeginTime = getTimeOfDate(js.getWeekBeginDate());


			if (nowTime == dayBeginTime) { // 检查运行时间。
				executeTask(task); // 任务已经超过一天，运行任务。
			}

		}

	}*/

	/**
	 * 取日期的时间部分。
	 * 
	 * @date 2014-4-30
	 * @author jiwei.zhang
	 * @param date
	 * @return
	 */
	/*private long getTimeOfDate(Date date) {
		String t = DateUtil.format("HH:mm:ss", date);
		long time = 0;
		try {
			time = DateUtil.parse("HH:mm:ss", t).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return time;
	}*/

	/**
	 * 判断是否是同一天
	 * @param date
	 * @param date2
	 * @return
	 */
	/*private boolean isSameDay(Date date,Date date2){
		String t1 = DateUtil.format("yyyy-MM-dd",date);
		String t2 = DateUtil.format("yyyy-MM-dd",date2);
		try {
			long time1 = DateUtil.parse("yyyy-MM-dd",t1).getTime();
			long time2 = DateUtil.parse("yyyy-MM-dd",t2).getTime();
			if(time1==time2){
				return true;
			} else {
				return false;
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return false;
	}*/
	/**
	 * 执行一个任务。
	 * 
	 * @date 2014-4-30
	 * @author jiwei.zhang
	 * @param task
	 *            任务定义 。
	 */
	/*private void executeTask(final SparkTask task) {
		log.info("启动调度任务："+task);
		pool.execute(task); // 使用线程池运行。
		task.getJob().setLastScheduledDate(new Date()); // 更新上次运行时间
		// 更新上次运行日期到日志表。
		jdbcTemplate.update("update ds_sys_job_schedule set LAST_SCHEDULED_DATE=? where id=?",new Timestamp(System.currentTimeMillis()),task.getJob().getId());
	}*/

}
