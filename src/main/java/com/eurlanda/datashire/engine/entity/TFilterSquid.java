package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.spark.squid.TJoinSquid;
import com.eurlanda.datashire.engine.util.SquidUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple3;

import java.util.List;
import java.util.Map;

/**
 * 过滤
 * Created by Juntao.Zhang on 2014/5/6.
 */
public class TFilterSquid extends TSquid {
    private TSquid previousSquid;

    TFilterExpression filterExpression;

    private List<Map<Integer, TStructField>> id2Columns;
    // 当前squid没有join,则需要该属性
    private String squidName;

	public TFilterSquid() {
        setType(TSquidType.FILTER_SQUID);
    }

    @Override
    public Object run(JavaSparkContext jsc) throws EngineException  {
        if (!previousSquid.isFinished()) {
            previousSquid.runSquid(jsc);
        }
        if (previousSquid.getOutRDD() == null) {
            return false;
        }
        /**
        outRDD = previousSquid.getOutRDD().filter(new Function<Map<Integer, DataCell>, Boolean>() {
            @Override
            public Boolean call(Map<Integer, DataCell> map) throws Exception {
                if (filterExpression == null) return true;
                return ExpressionValidator.validate(filterExpression, map);
            }
        });
         */
        if(id2Columns != null) {    // 判断是否使用dataframe来过滤
            // 判断上游是否是为 TJoinSquid
            if(previousSquid.outDataFrame != null ) {
                outDataFrame = ((TJoinSquid)previousSquid).outDataFrame.filter(filterExpression.getSourceExpression());
            } else {
                // join 已经将dataFrame实现了, 所以不需要考虑将多个rdd进行过滤了
                outDataFrame = TJoinSquid.rddToDataFrame(getJobContext().getSparkSession(),
                        squidName, previousSquid.getOutRDD().rdd(),
                        id2Columns.get(0)).filter(filterExpression.getSourceExpression());
            }
            outRDD = TJoinSquid.dataFrameToRDD(outDataFrame, id2Columns).toJavaRDD();

        } else {
            outRDD = SquidUtil.filterRDD(previousSquid.getOutRDD(), filterExpression);

        }


        return true;
    }

    public TSquid getPreviousSquid() {
        return previousSquid;
    }

    public void setPreviousSquid(TSquid previousSquid) {
        this.previousSquid = previousSquid;
    }

    public TFilterExpression getFilterExpression() {
        return filterExpression;
    }

    public void setFilterExpression(TFilterExpression filterExpression) {
        this.filterExpression = filterExpression;
    }

    public List<Map<Integer, TStructField>> getId2Columns() {
        return id2Columns;
    }

    public void setId2Columns(
            List<Map<Integer, TStructField>> id2Columns) {
        this.id2Columns = id2Columns;
    }

    public String getSquidName() {
        return squidName;
    }

    public void setSquidName(String squidName) {
        this.squidName = squidName;
    }
}





