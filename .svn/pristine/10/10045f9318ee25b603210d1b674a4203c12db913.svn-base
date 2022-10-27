package com.eurlanda.datashire.engine.translation.sqlparser;

import com.eurlanda.datashire.engine.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.regex.Matcher;

/**
 * sql where 括号语法解析器。
 * 
 * @author Gene a>1 and b<2 and ( c<4 or d>5 )
 */
public class SqlParser {
	static Stack<Integer> sign = new Stack<>();
	private String[] oprators = new String[] { "AND", "OR" };

	/**
	 * 纯字符串。不包括括号
	 * 
	 * @param str
	 * @return
	 */
	private List<SqlNode> parseStr(String str) {
		List<SqlNode> ret = new ArrayList<>();
		str = str.trim();
		Matcher matcher = StringUtils.match(str, "(.+?)(and|or|$)");
		while (matcher.find()) {
			String cond = matcher.group();
			ConditionNode node = new ConditionNode();
			node.setContent(cond);
			ret.add(node);
		}
		return ret;
	}

	private static List<String> getBracket(String str) {
		List<String> results = new ArrayList<String>();

		for (int i = 0; i < str.length(); i++) {
			char ch = str.charAt(i);
			if (ch == '(') {
				sign.push(i);
			} else if (ch == ')') {
				if (sign.size() == 0)
					throw new RuntimeException("语句缺少左括号");
				int begin = sign.pop();
				if (sign.size() == 0) {
					String ret = str.substring(begin + 1, i);
					results.add(ret);
				}
			}
		}
		if (sign.size() > 0) {
			throw new RuntimeException("语句缺少右括号");
		}
		return results;
	}

	public List<SqlNode> parse(String str) {
		List<SqlNode> results = new ArrayList<SqlNode>();
		Stack<Integer> sign = new Stack<>();
		StringBuilder sb = new StringBuilder();
		int pos = 0;

		while (pos < str.length()) {
			char ch = str.charAt(pos);
			if (ch == '(') {
				sign.push(pos);

				// 遇到 括号，开始翻译左边的表达式。
				if (sb.length() > 0) {
					results.addAll(this.parseStr(sb.toString()));
				}
				sb = new StringBuilder();

				// 翻译括号中的内容
				String qh = "";
				while (pos < str.length()) {
					pos++;
					ch = str.charAt(pos);
					if (ch == '(') {
						sign.push(pos);
					} else if (ch == ')') {
						if (sign.size() == 0)
							throw new RuntimeException("语句缺少左括号");
						int begin = sign.pop();
						if (sign.size() == 0) {
							qh = str.substring(begin + 1, pos); // 括号中的内容。
							break;
						}
					}
				}
				if (sign.size() > 0) {
					throw new RuntimeException("语句缺少右括号");
				}

				BracketNode node = new BracketNode();
				node.setChilds(parse(qh));
				results.add(node);
			} else {
				sb.append(ch);
			}
			pos++;
		}
		if (sb.length() > 0) {
			results.addAll(this.parseStr(sb.toString()));
		}
		return results;
	}

	public static void main(String[] args) {
		SqlParser sp = new SqlParser();
		List<SqlNode> nodes = sp.parse("a>1 and (b<2 and (c>3))");
		print(nodes, 0);
	}

	public static void print(List<SqlNode> nodes, int n) {
		for (SqlNode node : nodes) {
			for (int i = 0; i < n; i++) {
				System.out.print("\t");
			}
			System.out.print(node);
			System.out.println();
			if (node instanceof BracketNode) {
				print(((BracketNode) node).getChilds(), ++n);
			}
		}
	}
}
