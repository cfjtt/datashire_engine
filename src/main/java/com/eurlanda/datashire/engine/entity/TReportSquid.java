package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.enumeration.JobModuleStatusEnum;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.service.RpcServerFactory;
import com.eurlanda.datashire.engine.util.EngineLogFactory;
import com.eurlanda.datashire.engine.util.JobLogUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

/**
 * 报表调用 squid
 * Created by Juntao.Zhang on 2014/4/15.
 */
public class TReportSquid extends TSquid {
    private static final long serialVersionUID = 1L;

    private static Log log = LogFactory.getLog(TReportSquid.class);

    private int squidFlowId;
    private int repositoryId;
    private List<TSquid> preSquidList = new ArrayList<>();

    public TReportSquid() {
        setType(TSquidType.REPORT_SQUID);
    }

    @Override
    public Boolean run(JavaSparkContext jsc) throws EngineException {
        if (CollectionUtils.isNotEmpty(preSquidList)) {
            for (TSquid tSquid : preSquidList) {
                if (tSquid != null && tSquid.getType() != TSquidType.DATA_FALL_SQUID && !tSquid.isFinished()) {
                    tSquid.runSquid(jsc);
                }
            }
        }
        JobLogUtil.addJobModuleStatus(getTaskId(), getSquidId(), JobModuleStatusEnum.STAGE01);
        try {
            RpcServerFactory.getReportService().onReportSquidFinished(getTaskId(), repositoryId, getSquidId());
        } catch (Exception e) {
            EngineLogFactory.logError(this, "调用报表接口异常", e);
            log.debug(e.getMessage(), e);
        }
        log.debug("调用report server " + getTaskId() + ", repId:" + repositoryId + ",squidId : " + getSquidId());
        return true;

    }

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

    public void from(TSquidFlow squidFlow) {
        setRepositoryId(squidFlow.getRepositoryId());
        setSquidFlowId(squidFlow.getId());
    }

    public List<TSquid> getPreSquidList() {
        return preSquidList;
    }

    public void setPreSquidList(List<TSquid> preSquidList) {
        this.preSquidList = preSquidList;
    }

    public void addPreSquid(TSquid squid) {
        this.preSquidList.add(squid);
    }
}
