package com.eurlanda.datashire.engine.schedule;

import java.sql.Time;
import java.util.Date;
/**
 * 任务调度定义。
 * @date 2014-4-30
 * @author jiwei.zhang
 *
 */
public class ScheduleInfo {
	private int id;
	private int squidFlowId;		//sf ID
	private int repositoryId;		//仓库ID
	private int squidId;		// squidID.
	private String scheduleType;	// 调度类型，可选DAY/WEEK/MONTH
	private Date scheduleBeginDate;			// 任务全局调度开始时间。
	private Date scheduleEndDate;			// 任务全局调度结束时间。
	private boolean scheduleValid=true;		//  是否允许调度。
	private int dayDely;		// 每xx天。
	private int dayRunCount;	// 每天运行xx次
	private Time dayOnceBeginDate; // 单次执行的开始日期。
	private Date dayBeginDate;	// 每天从几点开始运行
	private Date dayEndDate;	// 每天运行到几点结束
	private int dayRunDely;		// 每次运行时间间隔，任务启动后，多长时间运行一次。单位为分钟。
	private int weekDay;		// 星期几
	private Date weekBeginDate;		//  星期几，几点开始。如12:00:00
	private int monthDay;		// 每月几号
	private Date monthBeginDate;	// 每月开始的日期。 如2015年6月30日
	private Date lastScheduledDate;		// 上次调度时间

    // 发送的邮箱地址，如果emails为空，则不需要发送邮件
    private String emails;

	public int getSquidFlowId() {
		return squidFlowId;
	}
	public void setSquidFlowId(int squidFlowId) {
		this.squidFlowId = squidFlowId;
	}
	public int getRepositoryId() {
		return repositoryId;
	}
	public void setRepositoryId(int repositoryId) {
		this.repositoryId = repositoryId;
	}
	public String getScheduleType() {
		return scheduleType;
	}
	public void setScheduleType(String scheduleType) {
		this.scheduleType = scheduleType;
	}
	public Date getScheduleBeginDate() {
		return scheduleBeginDate;
	}
	public void setScheduleBeginDate(Date scheduleBeginDate) {
		this.scheduleBeginDate = scheduleBeginDate;
	}
	public Date getScheduleEndDate() {
		return scheduleEndDate;
	}
	public void setScheduleEndDate(Date scheduleEndDate) {
		this.scheduleEndDate = scheduleEndDate;
	}
	public boolean isScheduleValid() {
		return scheduleValid;
	}
	public void setScheduleValid(boolean scheduleValid) {
		this.scheduleValid = scheduleValid;
	}
	public int getDayDely() {
		return dayDely;
	}
	public void setDayDely(int dayDely) {
		this.dayDely = dayDely;
	}
	public int getDayRunCount() {
		return dayRunCount;
	}
	public void setDayRunCount(int dayRunCount) {
		this.dayRunCount = dayRunCount;
	}
	public Date getDayBeginDate() {
		return dayBeginDate;
	}
	public void setDayBeginDate(Date dayBeginDate) {
		this.dayBeginDate = dayBeginDate;
	}
	public Date getDayEndDate() {
		return dayEndDate;
	}
	public void setDayEndDate(Date dayEndDate) {
		this.dayEndDate = dayEndDate;
	}
	public int getDayRunDely() {
		return dayRunDely;
	}
	public void setDayRunDely(int dayRunDely) {
		this.dayRunDely = dayRunDely;
	}
	public int getWeekDay() {
		return weekDay;
	}
	public void setWeekDay(int weekDay) {
		this.weekDay = weekDay;
	}
	public Date getWeekBeginDate() {
		return weekBeginDate;
	}
	public void setWeekBeginDate(Date weekBeginDate) {
		this.weekBeginDate = weekBeginDate;
	}
	public int getMonthDay() {
		return monthDay;
	}
	public void setMonthDay(int monthDay) {
		this.monthDay = monthDay;
	}
	public Date getMonthBeginDate() {
		return monthBeginDate;
	}
	public void setMonthBeginDate(Date monthBeginDate) {
		this.monthBeginDate = monthBeginDate;
	}
	public Date getLastScheduledDate() {
		return lastScheduledDate;
	}
	public void setLastScheduledDate(Date lastScheduledDate) {
		this.lastScheduledDate = lastScheduledDate;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}

	public Time getDayOnceBeginDate() {
		return dayOnceBeginDate;
	}

	public void setDayOnceBeginDate(Time dayOnceBeginDate) {
		this.dayOnceBeginDate = dayOnceBeginDate;
	}

	public int getSquidId() {
		return squidId;
	}

    public String getEmails() {
        return emails;
    }

    public void setEmails(String emails) {
        this.emails = emails;
    }

    public void setSquidId(int squidId) {
		this.squidId = squidId;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
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
		ScheduleInfo other = (ScheduleInfo) obj;
		if (id != other.id)
			return false;
		return true;
	}

    @Override public String toString() {
        return "ScheduleInfo{" +
                "id=" + id +
                ", squidFlowId=" + squidFlowId +
                ", repositoryId=" + repositoryId +
                ", squidId=" + squidId +
                ", scheduleType='" + scheduleType + '\'' +
                ", scheduleBeginDate=" + scheduleBeginDate +
                ", scheduleEndDate=" + scheduleEndDate +
                ", scheduleValid=" + scheduleValid +
                ", dayDely=" + dayDely +
                ", dayRunCount=" + dayRunCount +
                ", dayOnceBeginDate=" + dayOnceBeginDate +
                ", dayBeginDate=" + dayBeginDate +
                ", dayEndDate=" + dayEndDate +
                ", dayRunDely=" + dayRunDely +
                ", weekDay=" + weekDay +
                ", weekBeginDate=" + weekBeginDate +
                ", monthDay=" + monthDay +
                ", monthBeginDate=" + monthBeginDate +
                ", lastScheduledDate=" + lastScheduledDate +
                ", emails='" + emails + '\'' +
                '}';
    }
}
