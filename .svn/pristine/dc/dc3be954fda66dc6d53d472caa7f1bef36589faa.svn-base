package com.eurlanda.datashire.engine.util.cool;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;

import java.util.ArrayList;
/**
 * 流畅list
 * @author Gene
 *
 * @param <E>
 */
public class JList<E> extends ArrayList<E> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * 添加并返回自己
	 * @param obj
	 * @return
	 */
	public  JList<E> a(E obj){
		add(obj);
		return this;
	}
	/**
	 * 用对象数据初始化一个数组。
	 * @param objs
	 */
	public JList(E ... objs){
		for(E obj:objs){
			this.add(obj);
		}
	}
	/**
	 * 使用json形式，构造一个list 
	 * @param initStr 如[1,2,3,4,5] 或者["join","jerry","gene"]
	 */
	public JList(String initStr){
		JSONArray arr = JSON.parseArray(initStr);
		for(Object obj:arr){
			this.add((E) obj);
		}
	}
	/**
	 * 创建一个新list.
	 * @param vals
	 * @return
	 */
	public static JList create(Object ... vals){
		return new JList<>(vals);
	}
	public JList(){}
}
