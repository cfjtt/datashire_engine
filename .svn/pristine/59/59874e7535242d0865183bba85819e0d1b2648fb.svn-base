package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.utility.StringUtil;
import org.junit.Test;
import org.nfunk.jep.JEP;
import org.nfunk.jep.type.NULL;

import java.util.ArrayList;

/**
 * Created by zhudebin on 15-3-9.
 */
public class ExpressionValicatorTest {

    @Test
    public void test1() {
        String exp = "e_Production$LotSize>$lostSize";
        exp = StringUtil.ReplaceVariableForNameToJep(exp, new ArrayList<String>());
//        exp = StringUtil.ReplaceVariableForName(exp, new ArrayList<String>());
        System.out.println(exp);

        JEP myParser = new JEP();
        myParser.addConstant("null", NULL.build());
        myParser.addVariable("e_Production$LotSize", 30);
        myParser.addVariable("$lostSize", 20);
        myParser.parseExpression(exp);
        double result = myParser.getValue();
        System.out.println(result);
    }

    @Test
    public void test2() {
//        String exp = "'aaaaa'+'bbb'='cc'";
//        String exp = "3+2";
//        String exp = "\"foo\" + \"bar\" == \"foobar\"";
//        String exp = "\"foo\" + \"bar\"";
//        String exp = "\"foo\" + \"bar\" + $str";
        String exp = "'a' + $str";


        JEP myParser = new JEP();
        myParser.addConstant("null", NULL.build());
        myParser.addVariable("$str", 'c');
        myParser.parseExpression(exp);
        Object obj = myParser.getValueAsObject();
        System.out.println(obj);
        /**
        double result = myParser.getValue();
        System.out.println(result);
         */
    }
}
