package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.util.ListUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.Map;

/**
 * Created by zhudebin on 15-10-12.
 */
public class TDestHdfsSquid extends TSquid {

    private static Log log = LogFactory.getLog(TDestHdfsSquid.class);

    private List<Integer> idList;
    private Map<String, String> params;
    private int fileType;

    {
        this.setType(TSquidType.DEST_HDFS_SQUID);
    }

    private TSquid previousSquid;


    public TDestHdfsSquid() {
        this.setType(TSquidType.DEST_HDFS_SQUID);
    }

    @Override protected Object run(JavaSparkContext jsc) throws EngineException {
        log.info("落地ID集合为" + ListUtil.mkString(idList, "", ",", ""));
//        HdfsSquidUtil.saveToHdfsByPartitions(jsc.sc(), previousSquid.getOutRDD(), idList, params, fileType);

        return null;
    }

    public TSquid getPreviousSquid() {
        return previousSquid;
    }

    public void setPreviousSquid(TSquid previousSquid) {
        this.previousSquid = previousSquid;
    }

    public List<Integer> getIdList() {
        return idList;
    }

    public void setIdList(List<Integer> idList) {
        this.idList = idList;
    }

    public Map<String, String> getParams() {
        return params;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }

    public int getFileType() {
        return fileType;
    }

    public void setFileType(int fileType) {
        this.fileType = fileType;
    }
}
