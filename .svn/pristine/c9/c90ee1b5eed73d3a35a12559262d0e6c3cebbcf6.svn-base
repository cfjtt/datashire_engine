package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.exception.EngineException;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by zhudebin on 14-5-24.
 * 获取异常处理RDD
 */
public class TExceptionSquid extends TSquid {

    private TSquid previousSquid;

    public TExceptionSquid() {
        this.setType(TSquidType.EXCEPTION_SQUID);
    }

    @Override
    public Object run(JavaSparkContext jsc) throws EngineException  {
        if(!(previousSquid instanceof TExceptionDataSquid)) {
            throw new RuntimeException("类型不匹配 TExceptionDataSquid squid");
        }
        if(!previousSquid.isFinished()){
            previousSquid.runSquid(jsc);
        }
        this.outRDD = ((TExceptionDataSquid)previousSquid).getExpOutRDD();
        return this.outRDD;
    }

    public void setPreviousSquid(TSquid previousSquid) {
        this.previousSquid = previousSquid;
    }
}
