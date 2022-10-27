package com.eurlanda.datashire.engine.schedule;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.alibaba.fastjson.JSON;
import com.eurlanda.datashire.adapter.HyperSQLManager;
import com.eurlanda.datashire.engine.service.RpcServerFactory;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.Url;
import com.eurlanda.datashire.entity.WeiboSquid;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.utility.EnumException;
import org.apache.avro.AvroRemoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * spark任务。
 * 
 * @date 2014-4-30
 * @author jiwei.zhang
 * 
 */
public class SquidTask extends SparkTask {
	private Logger logger = LoggerFactory.getLogger(SquidTask.class);

	public SquidTask(ScheduleInfo job) {
		super(job);
	}

	@Override
	public void run() {
		int sid = this.getJob().getSquidId();
		// 准备构造爬虫任务。
        Map<String, Object> squid = null;
        try{

            squid = jdbc.queryForMap("select id,filter,squid_type_id from ds_squid where id=?", sid);
        } catch (Exception e) {
            logger.error("不存在该squid");
            return;
        }
		Integer squid_type_id = (Integer) squid.get("squid_type_id");
		Integer squidId = (Integer) squid.get("id");
		try {
			switch (SquidTypeEnum.valueOf(squid_type_id)) {
			case WEB:
				crawlWeb(squidId,this.getJob().getRepositoryId());
				break;
			case WEIBO:
				crawlWeibo(squidId,this.getJob().getRepositoryId());
				break;
			default:
				logger.error("参与调度的squid不是web或微博，id:{}", squidId);
			}

		} catch (EnumException e) {
			logger.error("错误的squid类型", e);
		}

	}

	private void crawlWeb(Integer squidId,Integer repId) {
		String sql = "select * from ds_url where squid_id="+squidId;
		List<Url> urls = null;
		try {
			urls = HyperSQLManager.query2List(jdbc.getDataSource().getConnection(), true, sql, null, Url.class);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		List<Map<String,Object>> profileList = new ArrayList<Map<String,Object>>();
		for (Url url : urls) {
			Map<String,Object> params = new HashMap<String, Object>();
			params.put("deep", url.getMax_fetch_depth());
			params.put("initurls", url.getUrl());
			params.put("limitHost", true);
			params.put("profilename", "profile-web-"+url.getId());
			params.put("squidId", squidId);
			params.put("repositoryId", repId);
			profileList.add(params);
		}
		String strProfile = JSON.toJSONString(profileList);
		try {
            RpcServerFactory.getCrawlerService().crawlWeb(strProfile);
		} catch (AvroRemoteException e) {
			e.printStackTrace();
		}

		logger.info("已经启动Web任务调度。squid[{}],任务列表:[{}]", squidId,strProfile);
	}

	private void crawlWeibo(Integer squidId,Integer repId){
		WeiboSquid weibo = null;
		Squid squid=null;
		String sql = "select * from ds_weibo_connection where id="+squidId;
		String squidSql="select * from DS_SQUID where id="+squidId;
		try {
			squid= HyperSQLManager.query2List(jdbc.getDataSource().getConnection(), true, squidSql, null, Squid.class).get(0);
			weibo = HyperSQLManager.query2List(jdbc.getDataSource().getConnection(), true, sql, null, WeiboSquid.class).get(0);
			weibo.setFilter(squid.getFilter());
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		List<Map<String,Object>> profileList = new ArrayList<Map<String,Object>>();
		if(ValidateUtils.isEmpty(weibo.getFilter())){
			throw new RuntimeException("关键字筛选不能为空。");
		}
		
		
		for (String key :weibo.getFilter().split(",")) {
			Map<String,Object> params = new HashMap<String, Object>();
			params.put("initurls",key);
			params.put("profilename", "profile-weibo-"+weibo.getId());
			params.put("searchBegin",weibo.getStart_data_date());
			params.put("searchEnd", weibo.getEnd_data_date());
			params.put("squidId", squidId);
			params.put("repositoryId", repId);
            params.put("username", weibo.getUser_name());
            params.put("password", weibo.getPassword());
			profileList.add(params);
		}
		
		String strProfile = JSON.toJSONString(profileList);
		try {
            RpcServerFactory.getCrawlerService().crawlWeibo(strProfile);
		} catch (AvroRemoteException e) {
			e.printStackTrace();
		}

		logger.info("已经启动WeiBo任务调度。squid[{}],任务列表:[{}]", squidId,strProfile);
	}
}
