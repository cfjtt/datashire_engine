package com.eurlanda.datashire.engine.translation;

import cn.com.jsoft.jframe.utils.DateTimeUtils;
import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.engine.util.StringUtils;
import com.eurlanda.datashire.utility.StringUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;

public class FilterTest {

	private static Log log = LogFactory.getLog(FilterTest.class);

	@Test
	public void test() {
		List<String> list = new ArrayList<>();
		String filter = "SELECT \"JGSJ\",\"CPHM\" from NJ_LOAD_DATA_05 where KKMC=@kkmc AND JGSJ BETWEEN @jgsj_begin AND @jgsj_end";
		String str = StringUtil.ReplaceVariableForName(filter, list);
		log.info(str);
		log.info(list);
	}

	public static void main(String[] args) throws Exception {
		String fs = "#a>6 and b.c=5 and (s1.beginDate >'2014-5-31' or s1.endDate >'2014-7-31')";
		if (!ValidateUtils.isEmpty(fs)) {
			// 将 sql表达式转换为 java表达式。
			fs = fs.replaceAll("=", " == ").replaceAll("(?i)and", " && ").replaceAll("(?i)or", " || ").replaceAll("#|(\\w+)\\.(?!\\d+)", "$1\\$");

			// 预处理时间，将时间转换成long 类型。
			Matcher m = StringUtils.match(fs, "'([\\d\\:\\-\\s\\.]+?)'");
			String tmp = fs;
			while (m.find()) {
				String expVal = m.group(1);
				Date date = null;
				if (!ValidateUtils.isNumeric(expVal)) {
					try {
						date = DateTimeUtils.parserDateTime(expVal, "yyyy-MM-dd hh:mm:ss.SSS");
					} catch (Exception e3) {
						try {
							date = DateTimeUtils.parserDateTime(expVal, "yyyy-MM-dd hh:mm:ss");
						} catch (Exception e2) {
							try {
								date = DateTimeUtils.parserDateTime(expVal, "yyyy-MM-dd");
							} catch (Exception e) {
								throw new RuntimeException("不支持的日期格式："+expVal);
							}

						}
					}
					
				}
				if (date != null) {
					tmp = tmp.replace(m.group(),date.getTime() + "");
				}
			}
			System.out.println(tmp);
		}
	}
}
