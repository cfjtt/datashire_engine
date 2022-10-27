package com.eurlanda.datashire.engine.schedule;

import com.eurlanda.datashire.engine.util.ConstantUtil;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * spark任务。
 * 
 * @date 2014-4-30
 * @author jiwei.zhang
 * 
 */
public abstract class SparkTask implements Runnable{
    protected JdbcTemplate jdbc;
    private ScheduleInfo job;

    public SparkTask(ScheduleInfo job) {
        this.job = job;
        int repId = this.getJob().getRepositoryId();
        jdbc = ConstantUtil.getJdbcTemplate();
    }

	public ScheduleInfo getJob() {
		return job;
	}

	public void setJob(ScheduleInfo job) {
		this.job = job;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((job == null) ? 0 : job.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SparkTask other = (SparkTask) obj;
		if (job == null) {
			if (other.job != null)
				return false;
		} else if (!job.equals(other.job))
			return false;
		return true;
	}

    @Override public String toString() {
        return "SparkTask{" +
                "job=" + job +
                '}';
    }
}
