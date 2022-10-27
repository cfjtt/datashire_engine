package com.eurlanda.datashire.engine.util;

import com.eurlanda.datashire.engine.entity.DataCell;
import com.eurlanda.datashire.engine.entity.TDataType;
import com.eurlanda.datashire.engine.entity.TFilterExpression;
import com.eurlanda.datashire.engine.entity.TTransformation;
import com.eurlanda.datashire.engine.enumeration.TTransformationInfoType;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * transformation type 方法实现的Test
 * Created by Juntao.Zhang on 2014/5/12.
 */
public class TransformationTypeAdaptorTest {
    protected static ArrayList<Integer> keyList(Integer... inKeys) {
        ArrayList<Integer> inList = new ArrayList<Integer>();
        Collections.addAll(inList, inKeys);
        return inList;
    }

    protected static Set<Integer> keySet(Integer... inKeys) {
        Set<Integer> in = new HashSet<>();
        Collections.addAll(in, inKeys);
        return in;
    }

    @Test
    public void testUpper() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.UPPER);
        tTransformation.addInKey(1);
        tTransformation.addOutKey(2);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.STRING, "Juntao,Zhang"));
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(in);
        Assert.assertEquals("JUNTAO,ZHANG", in.get(2).getData());
    }

    @Test
    public void testLower() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.LOWER);
        tTransformation.addInKey(1);
        tTransformation.addOutKey(2);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.STRING, "Juntao,Zhang"));
        Map<Integer, DataCell> out = new HashMap<>();
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
        Assert.assertEquals("juntao,zhang", out.values().iterator().next().getData());
    }

    @Test
    public void testAscii() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.ASCII);
        tTransformation.addInKey(1);
        tTransformation.addOutKey(2);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.STRING, "#"));
        Map<Integer, DataCell> out = new HashMap<>();
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
        Assert.assertEquals((int) '#', out.values().iterator().next().getData());
    }

    @Test
    public void testSimilarity() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.SIMILARITY);
        tTransformation.addInKey(1);
        tTransformation.addInKey(2);
        tTransformation.addOutKey(3);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(2, new DataCell(TDataType.STRING, "Zhang.Juntao.hans"));
        in.put(1, new DataCell(TDataType.STRING, "zhang.juntao"));
        Map<Integer, DataCell> out = new HashMap<>();
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
    }

    @Test
    public void testPatternIndex() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.PATTERNINDEX);
        tTransformation.addInKey(1);
        tTransformation.addInKey(2);
        HashMap<String, Object> infoMap = new HashMap<>();
        tTransformation.setInfoMap(infoMap);
        tTransformation.addOutKey(3);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.STRING, "han"));
        in.put(2, new DataCell(TDataType.STRING, "1.hans.han"));
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(in);
    }

    @Test
    public void testReplicate() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.REPLICATE);
        tTransformation.addInKey(1);
        HashMap<String, Object> infoMap = new HashMap<>();
        infoMap.put(TTransformationInfoType.REPEAT_COUNT.dbValue, 2);
        tTransformation.setInfoMap(infoMap);
        tTransformation.addOutKey(3);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.STRING, "hans"));
        Map<Integer, DataCell> out = new HashMap<>();
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
        Assert.assertEquals("hanshans", out.values().iterator().next().getData());
    }

    @Test
    public void testNumericToString() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.NUMERICTOSTRING);
        tTransformation.addInKey(1);
        tTransformation.addOutKey(3);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.DOUBLE, 123.23d));
        Map<Integer, DataCell> out = new HashMap<>();
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
        Assert.assertEquals("123.23", out.values().iterator().next().getData());
    }

    @Test
    public void testStringToNumeric() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.STRINGTONUMERIC);
        tTransformation.addInKey(1);
        tTransformation.addOutKey(3);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.STRING, "123.23"));
        Map<Integer, DataCell> out = new HashMap<>();
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
        Assert.assertEquals(123.23d, out.values().iterator().next().getData());
    }

    @Test
    public void testReplace() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.REPLACE);
        tTransformation.addInKey(1);
        HashMap<String, Object> infoMap = new HashMap<>();
        infoMap.put(TTransformationInfoType.REPLACEMENT_OLD_STRING.dbValue, ".");
        infoMap.put(TTransformationInfoType.REPLACEMENT_NEW_STRING.dbValue, "-");
        tTransformation.setInfoMap(infoMap);
        tTransformation.addOutKey(4);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.STRING, "123.23"));
        Map<Integer, DataCell> out = new HashMap<>();
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
        Assert.assertEquals("123-23", out.values().iterator().next().getData());
    }

    @Test
    public void testLeft() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.LEFT);
        tTransformation.addInKey(1);
        tTransformation.addOutKey(4);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.STRING, "123.23"));
        HashMap<String, Object> infoMap = new HashMap<>();
        infoMap.put(TTransformationInfoType.LENGTH.dbValue, "6");
        tTransformation.setInfoMap(infoMap);
        Map<Integer, DataCell> out = new HashMap<>();
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
        Assert.assertEquals("", out.values().iterator().next().getData());
        infoMap = new HashMap<>();
        infoMap.put(TTransformationInfoType.LENGTH.dbValue, "5");
        tTransformation.setInfoMap(infoMap);
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
        Assert.assertEquals("3", out.values().iterator().next().getData());
        infoMap = new HashMap<>();
        infoMap.put(TTransformationInfoType.LENGTH.dbValue, "7");
        tTransformation.setInfoMap(infoMap);
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
        Assert.assertEquals("", out.values().iterator().next().getData());
    }

    @Test
    public void testRight() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.RIGHT);
        tTransformation.addInKey(1);
        tTransformation.addOutKey(4);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.STRING, "123.23"));
        HashMap<String, Object> infoMap = new HashMap<>();
        infoMap.put(TTransformationInfoType.LENGTH.dbValue, "6");
        tTransformation.setInfoMap(infoMap);
        Map<Integer, DataCell> out = new HashMap<>();
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
        Assert.assertEquals("", out.values().iterator().next().getData());
        infoMap = new HashMap<>();
        infoMap.put(TTransformationInfoType.LENGTH.dbValue, "4");
        tTransformation.setInfoMap(infoMap);
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
        Assert.assertEquals("12", out.values().iterator().next().getData());
        infoMap = new HashMap<>();
        infoMap.put(TTransformationInfoType.LENGTH.dbValue, "7");
        tTransformation.setInfoMap(infoMap);
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
        Assert.assertEquals("", out.values().iterator().next().getData());
    }

    @Test
    public void testSubstring() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.SUBSTRING);
        tTransformation.addInKey(1);
        tTransformation.addOutKey(4);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.STRING, "zhang.juntao"));
        HashMap<String, Object> infoMap = new HashMap<>();
        infoMap.put(TTransformationInfoType.START_POSITION.dbValue, "3");
        infoMap.put(TTransformationInfoType.LENGTH.dbValue, "5");
        tTransformation.setInfoMap(infoMap);
        Map<Integer, DataCell> out = new HashMap<>();
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
        Assert.assertEquals("ng.ju", out.values().iterator().next().getData());
        infoMap = new HashMap<>();
        infoMap.put(TTransformationInfoType.START_POSITION.dbValue, "13");
        infoMap.put(TTransformationInfoType.LENGTH.dbValue, "2");
        tTransformation.setInfoMap(infoMap);
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
        Assert.assertEquals("", out.values().iterator().next().getData());
        infoMap = new HashMap<>();
        infoMap.put(TTransformationInfoType.START_POSITION.dbValue, "4");
        infoMap.put(TTransformationInfoType.LENGTH.dbValue, "20");
        tTransformation.setInfoMap(infoMap);
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
        Assert.assertEquals("g.juntao", out.values().iterator().next().getData());
    }

    @Test
    public void testReverse() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.REVERSE);
        tTransformation.addInKey(1);
        tTransformation.addOutKey(3);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.STRING, "hans"));
        Map<Integer, DataCell> out = new HashMap<>();
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
        String result = (String) out.values().iterator().next().getData();
        Assert.assertEquals("snah", result);
    }

    @Test
    public void testRightTrim() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.RIGHTTRIM);
        tTransformation.addInKey(1);
        tTransformation.addOutKey(3);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.STRING, " hans "));
        Map<Integer, DataCell> out = new HashMap<>();
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
        String result = (String) out.values().iterator().next().getData();
        Assert.assertEquals(" hans", result);
    }

    @Test
    public void testDatetimeToString() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.DATETIMETOSTRING);
        tTransformation.addInKey(1);
        tTransformation.addOutKey(3);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.TIMESTAMP, new java.sql.Timestamp(new Date().getTime())));
        Map<Integer, DataCell> out = new HashMap<>();
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
    }

    @Test
    public void testStringToDatetime() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.STRINGTODATETIME);
        tTransformation.addInKey(1);
        tTransformation.addOutKey(3);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.STRING, "2014-05-13 10:01:43[.663]"));
        Map<Integer, DataCell> out = new HashMap<>();
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
    }

    @Test
    public void testYear() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.YEAR);
        tTransformation.addInKey(1);
        tTransformation.addOutKey(3);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.TIMESTAMP, new Date()));
        Map<Integer, DataCell> out = new HashMap<>();
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
    }

    @Test
    public void testMonth() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.MONTH);
        tTransformation.addInKey(1);
        tTransformation.addOutKey(3);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.TIMESTAMP, new Date()));
        Map<Integer, DataCell> out = new HashMap<>();
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
    }

    @Test
    public void testDay() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.DAY);
        tTransformation.addInKey(1);
        tTransformation.addOutKey(3);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.TIMESTAMP, new java.sql.Timestamp(new Date().getTime())));
        Map<Integer, DataCell> out = new HashMap<>();
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
    }

    @Test
    public void testEXP() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.EXP);
        tTransformation.addInKey(1);
        tTransformation.addOutKey(3);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.DOUBLE, 3.202d));
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(in);
    }

    @Test
    public void testTermextract() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.TERMEXTRACT);
        tTransformation.addInKey(1);
        tTransformation.addOutKey(3);
        HashMap<String, Object> infoMap = new HashMap<>();
        infoMap.put(TTransformationInfoType.REG_EXPRESSION.dbValue, "a[a-z]+b");
        infoMap.put(TTransformationInfoType.TERM_INDEX.dbValue, "3");
        tTransformation.setInfoMap(infoMap);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.STRING, "ba##bfooab aob f aoob rrra$$$b***aooob"));
        Map<Integer, DataCell> out = new HashMap<>();
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
    }

    @Test
    public void testPatternindex() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.PATTERNINDEX);
        tTransformation.addInKey(1);
        tTransformation.addOutKey(3);
        HashMap<String, Object> infoMap = new HashMap<>();
        infoMap.put(TTransformationInfoType.REG_EXPRESSION.dbValue, "a[a-z]+b");
        tTransformation.setInfoMap(infoMap);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.STRING, "ba##bfooab aob f aoob rrra$$$b***aooob"));
        Map<Integer, DataCell> out = new HashMap<>();
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
        Assert.assertEquals(11, out.get(3).getData());
    }

    @Test
    public void testDatediff() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.DATEDIFF);
        tTransformation.addInKey(1);
        tTransformation.addInKey(2);
        tTransformation.addOutKey(3);
        HashMap<String, Object> infoMap = new HashMap<>();
        infoMap.put(TTransformationInfoType.DIFFERENCE_TYPE.dbValue, 2);
        tTransformation.setInfoMap(infoMap);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.TIMESTAMP, new java.sql.Timestamp(new Date("2014/5/15").getTime())));
        in.put(2, new DataCell(TDataType.TIMESTAMP, new java.sql.Timestamp(new Date("2013/5/15").getTime())));
        TransformationTypeAdaptor.mapping(in, tTransformation);
        org.junit.Assert.assertEquals(12l, in.get(3).getData());//Todo Juntao.Zhang :branch merge 过来需要改
    }

    @Test
    public void testChoice() {
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.LONG, 3));
        in.put(2, new DataCell(TDataType.STRING, "ZhangSan"));
        in.put(3, new DataCell(TDataType.STRING, "Allmilmo - Shanghai Office"));
        in.put(4, new DataCell(TDataType.DOUBLE, 72.3));
        in.put(5, new DataCell(TDataType.DOUBLE, 63.2));
        in.put(6, new DataCell(TDataType.DOUBLE, 99));

        TTransformation id = new TTransformation();
        id.setId(1);
        id.setDbId(1);
        id.setType(TransformationTypeEnum.VIRTUAL);
        id.setInKeyList(keyList(1));
        id.setOutKeyList(keyList(11));
        TTransformation name = new TTransformation();
        name.setId(2);
        name.setDbId(1);
        name.setType(TransformationTypeEnum.UPPER);
        name.setInKeyList(keyList(2));
        name.setOutKeyList(keyList(12));
        TTransformation com_name = new TTransformation();
        com_name.setId(3);
        com_name.setDbId(1);
        com_name.setType(TransformationTypeEnum.LOWER);
        com_name.setInKeyList(keyList(3));
        com_name.setOutKeyList(keyList(13));
        TTransformation choice = new TTransformation();
        choice.setId(4);
        choice.setDbId(1);
        choice.setType(TransformationTypeEnum.CHOICE);
        choice.setInKeyList(keyList(4, 5, 6));
        choice.setOutKeyList(keyList(14));
        choice.setInputsFilter(new ArrayList<TFilterExpression>());
        String exp = "x>y";
        TFilterExpression filterExpression = new TFilterExpression();
        Map<String, Integer> keyMap = new HashMap<>();
        keyMap.put("x", 4);
        keyMap.put("y", 5);
        filterExpression.setKeyMap(keyMap);
        filterExpression.setExpression(exp);
        choice.getInputsFilter().add(filterExpression);

        filterExpression = new TFilterExpression();
        keyMap = new HashMap<>();
        keyMap.put("x", 4);
        keyMap.put("y", 6);
        filterExpression.setKeyMap(keyMap);
        filterExpression.setExpression(exp);
        choice.getInputsFilter().add(filterExpression);

        filterExpression = new TFilterExpression();
        keyMap = new HashMap<>();
        keyMap.put("x", 5);
        keyMap.put("y", 6);
        filterExpression.setKeyMap(keyMap);
        filterExpression.setExpression(exp);
        choice.getInputsFilter().add(filterExpression);

//        TransformationTypeAdaptor.mapping(in, id);
//        TransformationTypeAdaptor.mapping(in, name);
//        TransformationTypeAdaptor.mapping(in, com_name);
        TransformationTypeAdaptor.mapping(in, choice);
        System.out.println(in);

        Assert.assertEquals(99, in.get(14).getData());
    }

    private static List<TFilterExpression> createFilter(int left, int right1, int right2) {
        List<TFilterExpression> expressionScoreList = new ArrayList<>();
        TFilterExpression expression = new TFilterExpression();
//        expression.setLeftCell(new DataCell(TDataType.TCOLUMN, left));
//        expression.setRightCell(new DataCell(TDataType.TCOLUMN, right1));
//        expression.setAndOrEnum(TAndOrEnum.AND);
//        expression.setExpOperationEnum(TExpOperationEnum.GE);
        expressionScoreList.add(expression);
        expression = new TFilterExpression();
//        expression.setLeftCell(new DataCell(TDataType.TCOLUMN, left));
//        expression.setRightCell(new DataCell(TDataType.TCOLUMN, right2));
//        expression.setAndOrEnum(TAndOrEnum.AND);
//        expression.setExpOperationEnum(TExpOperationEnum.GE);
        expressionScoreList.add(expression);
        return expressionScoreList;
    }

    @Test
    public void testSign() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.SIGN);
        tTransformation.addInKey(1);
        tTransformation.addOutKey(3);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.DOUBLE, 121.123d));
        Map<Integer, DataCell> out = new HashMap<>();
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
    }

    @Test
    public void testFormatdate() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.FORMATDATE);
        tTransformation.addInKey(1);
        tTransformation.addOutKey(3);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.STRING, "2014/5-14 18:59"));
        HashMap<String, Object> infoMap = new HashMap<>();
        infoMap.put(TTransformationInfoType.DATE_FORMAT.dbValue, "yyyy/MM-dd HH:mm");
        tTransformation.setInfoMap(infoMap);
        Map<Integer, DataCell> out = new HashMap<>();
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
    }

    /**
     * hwj 测试  TERMEXTRACT2ARRAY { //按正则表达式提取字符串中所有匹配的字符串，如果没有匹配则返回null
     */
    @Test
    public  void testTERMEXTRACT2ARRAY() {
        TTransformation tTransformation = new TTransformation();
        tTransformation.setType(TransformationTypeEnum.TERMEXTRACT2ARRAY);
        tTransformation.addInKey(1);
        tTransformation.addOutKey(3);
        HashMap<String, Object> infoMap = new HashMap<>();
        infoMap.put(TTransformationInfoType.REG_EXPRESSION.dbValue, "a[a-z]+b");
      //  infoMap.put(TTransformationInfoType..dbValue, "oo");
        tTransformation.setInfoMap(infoMap);
        Map<Integer, DataCell> in = new HashMap<>();
        in.put(1, new DataCell(TDataType.STRING, "ba##bfooab aob f aoob rrra$$$b***aooob"));
        Map<Integer, DataCell> out = new HashMap<>();
        TransformationTypeAdaptor.mapping(in, tTransformation);
        System.out.println(out);
    }

    public static void main(String[] args) {
//        String REGEX = "d[a-z]*g";
//        Pattern p = Pattern.compile(REGEX);
//        String INPUT = "The g dog doog dooogsays meow. All dogs say meow.doooog";
//        Matcher m = p.matcher(INPUT);       // 获得匹配器对象
//        int i = 1;
//        while (m.find()) {
//            if (i++ == 3) {
//                System.out.println(INPUT.substring(m.start(), m.end()));
//                break;
//            }
//        }
      //  System.out.println(Math.floor(-0.980001111111d));
       new  TransformationTypeAdaptorTest().testTERMEXTRACT2ARRAY();
    }


}
