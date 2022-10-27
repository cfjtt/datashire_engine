package com.eurlanda.datashire.schedule;

import com.alibaba.fastjson.JSON;
import com.eurlanda.datashire.engine.schedule.ScheduleInfo;
import com.eurlanda.datashire.engine.schedule.ScheduleService;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ScheduleServiceTest {
	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	public void testSchedule() throws Exception {
		ScheduleService ss = ScheduleService.getInstance();

		ScheduleInfo job = new ScheduleInfo();
		job.setScheduleBeginDate(new Date());
		job.setScheduleEndDate(sdf.parse("2015-4-30 12:00:00"));
		job.setScheduleValid(true);
		job.setScheduleType("DAY");
		job.setDayDely(1);
		job.setDayRunCount(2);
		job.setDayRunDely(1);
		job.setDayBeginDate(new Date());
		job.setDayEndDate(sdf.parse("2014-4-30 22:00:00"));
		TestSparkTaskExecutor task = new TestSparkTaskExecutor(job);

		task.setJob(job);

		//ss.addSchedule(task);
	}

	public static void main2(String[] args) throws ParseException {
		ScheduleService ss = ScheduleService.getInstance();

		ScheduleInfo job = new ScheduleInfo();
		job.setScheduleBeginDate(new Date());
		job.setScheduleEndDate(sdf.parse("2015-4-30 12:00:00"));
		job.setScheduleValid(true);
		job.setScheduleType("DAY");
		job.setDayDely(1);
		job.setDayRunCount(2);
		job.setDayRunDely(1);
		job.setDayBeginDate(new Date());
		job.setDayEndDate(sdf.parse("2014-4-30 22:00:00"));

		TestSparkTaskExecutor task = new TestSparkTaskExecutor(job);

		//ss.addSchedule(task);
	}
	
	public static void main(String[] args) {
		Profile p = new Profile();
		p.setDeep(3);
		p.setIniturls("http://www.csdn.net/");
		p.setProfilename("csdn");
		String t = JSON.toJSONString(p);
		System.out.println(t);
	}
}
