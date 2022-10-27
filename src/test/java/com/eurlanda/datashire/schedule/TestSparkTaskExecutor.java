package com.eurlanda.datashire.schedule;

import com.eurlanda.datashire.engine.schedule.ScheduleInfo;
import com.eurlanda.datashire.engine.schedule.SparkTask;

public class TestSparkTaskExecutor extends SparkTask{


	public TestSparkTaskExecutor(ScheduleInfo job) {
		super(job);
		// TODO Auto-generated constructor stub
	}



	@Override
	public void run() {
		System.out.println("task executed");
	}

}
