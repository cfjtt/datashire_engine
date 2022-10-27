package com.eurlanda.datashire.engine.translation.sqlparser;

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
        map.p("a", 10);
		bind.putAll(map);
		
		engine.setBindings(bind, ScriptContext.ENGINE_SCOPE);
		long begin = System.currentTimeMillis();
		for (int i = 0; i < 1; i++) {
			map.p("a", 10);
            engine.put("a", 0);
			Object obj = engine.eval("a>1");
            System.out.println(obj);

		}

		System.out.println(" time used (ms):" + (System.currentTimeMillis() - begin));
	}

}
