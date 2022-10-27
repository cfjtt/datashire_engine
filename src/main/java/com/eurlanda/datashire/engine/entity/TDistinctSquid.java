package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.exception.EngineException;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 去重
 */
public class TDistinctSquid extends TSquid {
    private TSquid previousSquid;

    public TDistinctSquid() {
        setType(TSquidType.DISTINCT_SQUID);
    }

    @Override
    public Object run(JavaSparkContext jsc) throws EngineException {
        if (!previousSquid.isFinished()) {
            previousSquid.runSquid(jsc);
        }
        if (previousSquid.getOutRDD() == null) {
            return false;
        }
//        System.out.println("前一个squid 输出数据总数为：" + previousSquid.getOutRDD().count());
        outRDD = previousSquid.getOutRDD().distinct();
//        System.out.println("distinct squid 输出数据总数为：" + outRDD.count());
        return true;
    }

    public TSquid getPreviousSquid() {
        return previousSquid;
    }

    public void setPreviousSquid(TSquid previousSquid) {
        this.previousSquid = previousSquid;
    }

}





