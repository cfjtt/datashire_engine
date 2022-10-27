package com.eurlanda.datashire.engine.translation.expression;

import com.eurlanda.datashire.engine.entity.DataCell;
import com.eurlanda.datashire.engine.entity.TFilterExpression;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nfunk.jep.JEP;
import org.nfunk.jep.type.NULL;

import java.util.Map;

/**
 * squid表达式验证器。
 * @author Gene
 *
 */
public class ExpressionValidator{
    private static Log log = LogFactory.getLog(ExpressionValidator.class);

    /**
     * 检查一个表达式是否为true.
     * @param filterExpression 待执行的表达式。
     * @param data spark RDD data
     * @return
     */
    public static boolean validate(TFilterExpression filterExpression, Map<Integer, DataCell> data) {
        return validate(filterExpression.getExpression(), data, filterExpression.getKeyMap(), filterExpression.getVarMap());
    }

        /**
         * 检查一个表达式是否为true.
         * @param exp 待执行的表达式。
         * @param data spark RDD data
         * @param keyMapper 变量和RDD key的 映射 。
         * @param varMap 变量->值 映射
         * @return
         */
	public static boolean validate(String exp, Map<Integer, DataCell> data,Map<String,Integer> keyMapper, Map<String, Object> varMap) {
        // TODO 优化性能
        //判断是否存在中文，如果存在则不检查，默认是正确的
        /*if(StringUtils.isHaveChinese(exp)){
            return true;
        }*/
		JEP myParser = new JEP();
        myParser.addConstant("null", NULL.build());
        for(String keyName: keyMapper.keySet()){
            myParser.addVariable(keyName,null);
        }
        for(Map.Entry<String, Object> entry : varMap.entrySet()) {
            myParser.addVariable(entry.getKey(), entry.getValue());
        }
        myParser.parseExpression(exp);
		for(String keyName: keyMapper.keySet()){
			Integer key = keyMapper.get(keyName);
			DataCell cell = data.get(key);
			Object val = null;
            if(cell == null) {
                val= NULL.build();
            } else {
                val = cell.getData();
                if(val ==null){
                    val= NULL.build();
                }else if (val instanceof java.sql.Timestamp) {
                    val = ((java.sql.Timestamp) val).getTime();
                }else if (val instanceof java.sql.Time) {
                    val = ((java.sql.Time) val).getTime();
                }
            }

			myParser.addVariable(keyName, val);
		}

		Double val = myParser.getValue();

//        if(val.isNaN()){
//            throw new RuntimeException("表达式执行失败:"+exp + ",异常：不是数值、日期类型不能比较大小");
//        }
//		if( myParser.hasError()){
        if(val.isNaN() && myParser.hasError()){
            // 打印异常
            StringBuilder sb = new StringBuilder();
            for(String keyName: keyMapper.keySet()){
                Integer key = keyMapper.get(keyName);
                sb.append(data.get(key));
            }
            // 本地尝试一次
			throw new RuntimeException("表达式执行失败:"+exp + ",异常：" + myParser.getErrorInfo());
		}else{
			if(val.intValue()==1){
				return true;
			}
		}
		return false;
	}
}
