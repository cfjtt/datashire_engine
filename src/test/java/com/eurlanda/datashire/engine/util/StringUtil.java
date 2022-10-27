package com.eurlanda.datashire.engine.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

/**
 * Created by zhudebin on 14-6-22.
 */
public class StringUtil {
    private static Map<Integer, String> map = new HashMap<>();
    private static int idx = 0;

    public static void splitExp(String exp) {
        System.out.println(exp);
        // 将),( 替换成 ) ,(
        exp = exp.replace(")", ") ");
        exp = exp.replace("(", "( ");
        System.out.println(exp);
        String[] exps = exp.split(" ");
        Stack<String> stack = null;
        StringBuilder childStr = null;
        StringBuilder currentStr = new StringBuilder();

        for(String ex : exps) {
            ex = ex.trim();
            if(ex.trim().startsWith("(")) {
                if(stack == null) {
                    stack = new Stack<>();
                    childStr = new StringBuilder();
                    childStr.append(ex.substring(1)).append(" ");
                } else {
                    childStr.append(ex);
                }
                stack.push("(");
            }else if(ex.trim().endsWith(")")) {
                if(stack == null) {
                    throw new RuntimeException("字符串不正常....");
                } else if(stack.size() ==1) {
                    if(ex.length() == 1) {

                    } else {
                        childStr.append(ex.substring(0, ex.length()-1)).append(" ");
                    }
                } else {
                    childStr.append(ex).append(" ");
                }
                stack.pop();
                if(stack.size() ==0) {
                    System.out.println("子查询语句分离成功");
//                    stack = null;
                    System.out.println("子查询层：" + childStr.toString());
                    map.put(idx, childStr.toString());
                }
            }  else

            // 判断子查询语句是否分离
            if(stack == null )  {
                // 当前层的
                currentStr.append(ex).append(" ");
            } else if(stack != null && stack.size() == 0) {
                currentStr.append("$" + idx + "==$" + idx ).append(" ");
                idx ++;
                currentStr.append(ex).append(" ");
                stack = null;
            }
            else {
                // 子查询层
                childStr.append(ex).append(" ");
            }

        }

        if(stack != null && stack.size() == 0) {
            currentStr.append("$" + idx + "==$" + idx ).append(" ");
            idx ++;
            stack = null;
        }
        System.out.println("current : " + currentStr.toString());
    }

    public static void main(String[] args) {
        String s1 = "111==2 and 3>4 or (2==1 and 111>1100)";
        String s2 = "111==2 and 3>4 or (2==1 and 111>1100) or (200==1 or (1==1 and 2===1))";
        String s3 = "";
        String s4 = "";
        String s5 = "";
        splitExp(s2);
        System.out.println(map);
    }

}
