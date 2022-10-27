package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.exception.EngineException;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by zhudebin on 14-5-7.
 * 内部抽取的squid。在stagesquid中使用全量时会翻译
 */
public class TInnerExtractSquid extends TSquid {

    // 上一个TSquid, 现在只能是落地squid
    private TSquid preTSquid;
    // 从数据库中抽取，暂时只满足数据库落地抽取
    private TDatabaseSquid tDatabaseSquid;

    public TInnerExtractSquid() {
        this.setType(TSquidType.INNEREXTRACT_SQUID);
    }

    @Override
    public Object run(JavaSparkContext jsc) throws EngineException  {
        if(!preTSquid.isFinished()) {
            preTSquid.runSquid(jsc);
        }
        tDatabaseSquid.setCurrentFlow(this.getCurrentFlow());
        outRDD = tDatabaseSquid.run(jsc);
        return outRDD;
    }

    public TSquid getPreTSquid() {
        return preTSquid;
    }

    public void setPreTSquid(TSquid preTSquid) {
        this.preTSquid = preTSquid;
    }

    public TDatabaseSquid gettDatabaseSquid() {
        return tDatabaseSquid;
    }

    public void settDatabaseSquid(TDatabaseSquid tDatabaseSquid) {
        this.tDatabaseSquid = tDatabaseSquid;
    }
}
