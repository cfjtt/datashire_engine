package com.eurlanda.datashire.engine.translation.expression;

import com.eurlanda.datashire.engine.util.cool.JMap;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.SimpleScriptContext;

public class SqlparserTest {

	/**
	 * and a>5
	 * 
	 * 
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// 获取脚本引擎
		ScriptEngineManager mgr = new ScriptEngineManager();
		ScriptEngine engine = mgr.getEngineByName("javascript");
		// 绑定数据
		ScriptContext newContext = new SimpleScriptContext();
		Bindings bind = newContext.getBindings(ScriptContext.ENGINE_SCOPE);
		JMap<String, Object> map = new JMap<>();
		bind.putAll(map);
		// System.out.println(sql);
		engine.setBindings(bind, ScriptContext.ENGINE_SCOPE);
		long begin = System.currentTimeMillis();
		for (int i = 0; i < 100000; i++) {
//			String sql = "(#a>5) and (#ROUND_1<2 or (#CALCULATION_1 >8.0))";
//			sql = sql.replaceAll("and", " && ").replaceAll("or", " || ");
//			sql = sql.replace("#a", "1");
//			sql = sql.replace("#ROUND_1", "2");
//			sql = sql.replace("#CALCULATION_1", "3");
			String sql="5>1 && 4<8 &&( 2>5 ||9>7)";
			try {
				boolean t = 5>1;
				Object val = engine.eval(sql);
				
			} catch (Exception e) {
				throw e;
			}// end try
		}
		System.out.println(" time used (ms):" + (System.currentTimeMillis() - begin));
	}

}
