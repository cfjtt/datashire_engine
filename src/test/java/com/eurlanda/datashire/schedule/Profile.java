package com.eurlanda.datashire.schedule;


import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Profile entity. @author MyEclipse Persistence Tools
 */

public class Profile implements java.io.Serializable {

	// Fields

	private Integer id;
	private String profilename;
	private String initurls;
	private String fileex;
	private String filter;
	private Integer deep;
	private Integer threadnum;
	private Integer serverid;
	private Date adddate;
	private Integer isfinished;
	private Date lastfinishtime;
	private boolean isLimitHost=true;
	private Set<String> domainSet;
	private String keywords;
	private SourceType sourceType=SourceType.NORMAL_WEB;
	private String webType;
	
	// Constructors

	public String getWebType() {
		return webType;
	}

	public void setWebType(String webType) {
		this.webType = webType;
	}

	public boolean isLimitHost() {
		return isLimitHost;
	}

	public void setLimitHost(boolean isLimitHost) {
		this.isLimitHost = isLimitHost;
	}

	public Set<String> getHostSet() {
		if(this.domainSet==null){
			this.domainSet= new HashSet<String>();
			String tmp = this.getIniturls();
			// 初始化URL。
			if (tmp != null) {
				String[] initurls = tmp.split("\\,");
				if (initurls != null && initurls.length > 0) {
					for (int i = 0; i < initurls.length; i++) {
							String url = initurls[i];
							Pattern ptn = Pattern.compile("\\w+://([^/]+)[:\\d/]*");
							Matcher mt = ptn.matcher(url);
							if(mt.find()){
								this.domainSet.add(mt.group(1));
							}
					}
				}
			}
		}
		return domainSet;
	}

	/** default constructor */
	public Profile() {
	}

	/** minimal constructor */
	public Profile(Integer id, String profilename) {
		this.id = id;
		this.profilename = profilename;
	}

	/** full constructor */
	public Profile(Integer id, String profilename, 
			String initurls, String fileex, String filter, Integer deep,
			Integer threadnum, Integer serverid, Date adddate,
			Integer isfinished, Date lastfinishtime) {
		this.id = id;
		this.profilename = profilename;
		this.initurls = initurls;
		this.fileex = fileex;
		this.filter = filter;
		this.deep = deep;
		this.threadnum = threadnum;
		this.serverid = serverid;
		this.adddate = adddate;
		this.isfinished = isfinished;
		this.lastfinishtime = lastfinishtime;
	}

	// Property accessors

	public Integer getId() {
		return this.id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getProfilename() {
		return this.profilename;
	}

	public void setProfilename(String profilename) {
		this.profilename = profilename;
	}

	public String getIniturls() {
		return this.initurls;
	}

	public void setIniturls(String initurls) {
		this.initurls = initurls;
	}

	public String getFileex() {
		return this.fileex;
	}

	public void setFileex(String fileex) {
		this.fileex = fileex;
	}

	public String getFilter() {
		return this.filter;
	}

	public void setFilter(String filter) {
		this.filter = filter;
	}

	public Integer getDeep() {
		return this.deep;
	}

	public void setDeep(Integer deep) {
		this.deep = deep;
	}

	public Integer getThreadnum() {
		return this.threadnum;
	}

	public void setThreadnum(Integer threadnum) {
		this.threadnum = threadnum;
	}

	public Integer getServerid() {
		return this.serverid;
	}

	public void setServerid(Integer serverid) {
		this.serverid = serverid;
	}

	public Date getAdddate() {
		return this.adddate;
	}

	public void setAdddate(Date adddate) {
		this.adddate = adddate;
	}

	public Integer getIsfinished() {
		return this.isfinished;
	}

	public void setIsfinished(Integer isfinished) {
		this.isfinished = isfinished;
	}

	public Date getLastfinishtime() {
		return this.lastfinishtime;
	}

	public void setLastfinishtime(Date lastfinishtime) {
		this.lastfinishtime = lastfinishtime;
	}
	
	@Override
	public String toString() {
		return "Profile [profilename=" + profilename + ", initurls=" + initurls + "]";
	}

	public String getKeywords() {
		return keywords;
	}

	public void setKeywords(String keywords) {
		this.keywords = keywords;
	}

	public SourceType getSourceType() {
		return sourceType;
	}

	public void setSourceType(SourceType sourceType) {
		this.sourceType = sourceType;
	}
	/**
	 * 构造一个web的配置文件
	 * @param profilename 配置文件名称
	 * @param initurls 初始化URL，多个以逗号分隔
	 * @param deep 抓取深度
	 * @param isLimitHost 是否限制主机范围
	 */
	public Profile(String profilename, String initurls, Integer deep, boolean isLimitHost) {
		super();
		this.profilename = profilename;
		this.initurls = initurls;
		this.deep = deep;
		this.isLimitHost = isLimitHost;
		this.sourceType=SourceType.NORMAL_WEB;
	}
	/**
	 * 构造一个微博的配置文件
	 * @param profilename 配置文件名称
	 * @param keywords 关键字，多个以逗号隔开
	 */
	public Profile(String profilename, String keywords) {
		super();
		this.profilename = profilename;
		this.keywords = keywords;
		this.sourceType=SourceType.SINA_WEIBO;
	}
	
}