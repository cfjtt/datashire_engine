package com.eurlanda.datashire.engine.util.cool;

import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
/**
 * 流畅list
 * @author Gene
 *
 */
public class JMap<K,V> extends HashMap<K,V> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 添加并返回自己
	 * @param obj
	 * @return
	 */
	public  JMap<K,V> p(K key, V obj){
		put(key,obj);
		return this;
	}
	
	public JMap(String init){
		JSONObject obj = JSONObject.parseObject(init);
		for(Object key :obj.keySet()){
			this.put((K)key,(V) obj.get(key));
		}
	}
	/**
	 * 创建一个新list.
	 * @return
	 */
	public static JMap create(){
		return new JMap<>();
	}
	public JMap(){}
}
