package com.eurlanda.datashire.engine.util;

import com.eurlanda.datashire.engine.entity.TColumn;
import com.eurlanda.datashire.engine.entity.TDataType;
import com.eurlanda.datashire.engine.exception.EngineException;
import com.eurlanda.datashire.engine.translation.BuilderContext;
import com.eurlanda.datashire.entity.DSVariable;
import com.eurlanda.datashire.entity.Squid;

import java.util.Map;

/**
 * Created by zhudebin on 15-1-20.
 */
public class VariableUtil {

    public static Object variable2Value(DSVariable dsVariable) {
        return TColumn.toTColumnValue(dsVariable.getVariable_value(), TDataType.sysType2TDataType(dsVariable.getVariable_type()));
    }

    /**
     * 返回该变量的在翻译的map中的key
     * @param var 变量名字
     * @param squidId squid id
     * @return
     */
    public static String squidVariableKey(String var, int squidId) {
        return squidId + "_" + var;
    }

    /**
     * squidflow 中寻找变量
     * @param varName
     * @param ctx
     * @return
     */
    public static Object variableValue(String varName, BuilderContext ctx) {
        Map<String, DSVariable> variableMap = ctx.getVariable();
        DSVariable var = null;
        if((var = variableMap.get(varName)) != null) {
            return VariableUtil.variable2Value(var);
        } else {
            // 不存在该变量，参数错误，抛出异常
            throw new EngineException("squidflow 中使用该变量[" + varName + "]不存在");
        }
    }

    /**
     * squid 中寻找 变量
     * @param varName
     * @param variableMap
     * @param squid
     * @return
     */
    public static Object variableValue(String varName, Map<String, DSVariable> variableMap, Squid squid) {
        DSVariable var = null;
        if((var = variableMap.get(VariableUtil.squidVariableKey(varName, squid.getId()))) != null) {
            return VariableUtil.variable2Value(var);
        } else if((var = variableMap.get(varName)) != null) {
            return VariableUtil.variable2Value(var);
        } else {
            // 不存在该变量，参数错误，抛出异常
            throw new EngineException(squid.getName() + " 中使用该变量[" + varName + "]不存在");
        }
    }
}
