package com.eurlanda.datashire.engine.translation;

import com.alibaba.fastjson.JSON;
import org.junit.Test;

import java.util.Map;

public class JSONTest {
	@Test
	public void testParseJson() {
		String text = "{\"breakPoints\":[1,2,3,4],\"name\":\"xxxx\"}";
		Map<String,Integer[]> obj = (Map<String, Integer[]>) JSON.parse(text);
		Integer[] breakpoints = obj.get("breakPoints");
		System.out.println(breakpoints);
	}
}
