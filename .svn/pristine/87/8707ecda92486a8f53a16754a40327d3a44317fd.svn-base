package com.eurlanda.datashire.engine.util;

import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.language.Soundex;
import org.apache.commons.lang.NotImplementedException;
import scala.collection.mutable.StringBuilder;

/**
 * Todo:Juntao.Zhang 目前只实现了 最长公共子序列 算法
 * Created by Juntao.Zhang on 2014/5/12.
 */
enum SimilarityCalculation {
    //简单统计2个string重叠的字符的个数，除以2个string平均长度
    BASE {
        public double getResult(String s, String t) {
          /*  int len = org.apache.commons.lang.StringUtils.getLevenshteinDistance(s, t);
            float average = average(s, t);
            return  len / (average);
            */
           return BaseSrtingSimilarityRepeatedCount(s,t);//一个字符出现多次时，重复计数
           //return BaseSrtingSimilarityNotRepeatedCount(s,t);//一个字符出现多次时，不重复计数
        }
    },
    /**
     * @link http://en.wikipedia.org/wiki/Soundex
     * @link http://commons.apache.org/proper/commons-codec/archives/1.9/apidocs/index.html
     * Todo:Juntao.Zhang 中文支持
     * 解决思路:
     * 1.整理汉语拼音相似问题 @see <a href="http://wenku.baidu.com/link?url=zrpDivwwy7vYz5mwKL9jZf6KPviVp1AqfIR8RHXQnmjvlNto_z_GbZbXDHqCY5GiTa5Wv8o9kxgODQ-3XJfzX8FHnlD4QUSucYwuo3UQUVC">
     * 中文支持</a>
     * 2.每个字提取拼音
     * 3.改写LevenshteinDistance
     * 4.求拼音相似度
     *
     * http://baike.baidu.com/link?url=GGXSMm2pKH066lqkZTdmaZu0hpqa7xAXbyFx3gtt0czKhhwi-Pbzob9iEX2HddunKfaChkqtowSsbFlYx1_5nq
     * SOUNDEX 将 alpha 字符串转换成由四个字符组成的代码，以查找相似的词或名称。
     * 代码的第一个字符是 character_expression 的第一个字符，
     * 代码的第二个字符到第四个字符是数字。
     * 将忽略 character_expression 中的元音，除非它们是字符串的第一个字母。可以嵌套字符串函数。
     *
     * 规则编辑
    a e h i o u w y -> 0
    b f p v -> 1
    c g j k q s x z -> 2
    d t -> 3
    l -> 4
    m n -> 5
    r -> 6
    1、提取字符串的首字母作为soundex的第一个值。
    2、按照上面的字母对应规则，将后面的字母逐个替换为数字。如果有连续的相等的数字，只保留一个，其余的都删除掉。并去除所有的0。
    3、如果结果超过4位，取前四位；如果结果不足4位向后补0。
     *
     * 按照上述规则，计算字符串 “Ahnddreg" 的soundex过程为：
    1、去首字母 a
    2、替换后面的字母 a0533602。按照规则去掉所有的0，结果变为 a53362。再去掉重复的数字，结果变为a5362。
    3、现在结果总长为5位，我们只取前4位，所以结果为a536
     */
    SOUNDEX {
        Soundex enSoundex = new Soundex();
        public double getResult(String s, String t) {
            try {
                int re = enSoundex.difference(s, t);
                return re / 4.0f;
            } catch (EncoderException e) {
                e.printStackTrace();
            }
            return 0f;
        }
    },
    //  LCS算法
    LCS {
        public double getResult(String s, String t) {
            int len = org.apache.commons.lang.StringUtils.getLevenshteinDistance(s, t),
                    maxLen = max(s, t);
            return 1 - (len / (maxLen * 1.0f));
        }
    },
    // GST（Greedy String Tiling）算法是一种贪婪串匹配算法，
    // 这一算法对两个字符串进行贪婪式搜索以找出最大公有子串，
    // 它需要对要计算的两个字符串进行多次搜索，
    // 每次找出当前字符串中未“标注”部分的最长公共子串，
    // 并把找出的最长公共子串“标注”为已使用，避免最大匹配重复使用。
    GST {
        public double getResult(String s, String t) {
            throw new NotImplementedException();
        }
    };

    private static int max(String s, String t) {
        int sl = s.length();
        int tl = t.length();
        return sl > tl ? sl : tl;
    }

    private static double average(String s, String t) {
        int sl = s.length();
        int tl = t.length();
        return (sl + tl) / 2.0;
    }

    public double getResult(String s, String t) {
        throw new AbstractMethodError();
    }

    /**
     * 简单统计2个string相同的字符的个数，除以2个string平均长度
     * 一个字符出现多次时，重复计数
     * @param str1
     * @param str2
     * @return
     */
    public static double BaseSrtingSimilarityRepeatedCount( String str1, String str2) {
        if (str1 == null || str2 == null) {
            return 0;
        }
        if(str1.length() ==0 && str2.length() ==0){
            return 0;
        }
        if(str1.equals(str2)){
            return 1.0;
        }
        int commcount = 0;
        for (int i1=0;i1<str1.length();i1++) {
            for(int i2=0;i2<str2.length();i2++){
                if(str1.charAt(i1)== str2.charAt(i2)){
                    commcount++;
                }
            }
        }
        double average = average(str1, str2);
        return 1.0 * commcount / average;
    }

    /**
     * 简单统计2个string相同的字符的个数，除以2个string平均长度
     * 一个字符出现多次时，不重复计数
     * @param str1
     * @param str2
     * @return
     */
    public static double BaseSrtingSimilarityNotRepeatedCount( String str1, String str2) {
        if (str1 == null || str2 == null) {
            return 0;
        }
        if(str1.equals(str2)){
            return 1.0;
        }
        str1=removeRepeatedChars(str1);
        str2=removeRepeatedChars(str2);
        int commcount = 0;
        for (int i1=0;i1<str1.length();i1++) {
            for(int i2=0;i2<str2.length();i2++){
                if(str1.charAt(i1)== str2.charAt(i2)){
                    commcount++;
                }
            }
        }
        double average = average(str1, str2);
        return 1.0 * commcount / average;
    }


    /**
     * 去掉字符串中重复的字符
     * @param str
     * @return
     */
    public static String removeRepeatedChars(String str) {
        if (str== null)
            return str;
        StringBuilder sb = new StringBuilder();
        int i = 0, len = str.length();
        while (i < len) {
            char c = str.charAt(i);
            sb.append(c);
            i++;
            while (i < len && str.charAt(i) == c) {
                i++;
            }
        }
        return sb.toString();
    }

    /**
     *  Levenshtein 距离，又称编辑距离
     *  http://wdhdmx.iteye.com/blog/1343856
     * @param str1
     * @param str2
     * @return
     */
    public static double LevenshteinDistence( String str1, String str2) {
        int len = org.apache.commons.lang.StringUtils.getLevenshteinDistance(str1, str2);
        double average = average(str1, str2);
        return 1 - (len / (average));
    }

    public static void main(String[] args) {
        //要比较的两个字符串
//        String str1 = "hans0hans0";
//        String str2 = "ha1s1hans01";
//        System.out.println(SimilarityCalculation.LCS.getResult(str1, str2));
//        System.out.println(SimilarityCalculation.BASE.getResult("hamhans", "hams"));
        System.out.println(SimilarityCalculation.SOUNDEX.getResult("hans", "result"));
        System.out.println(SimilarityCalculation.SOUNDEX.getResult("bug", "bag"));
        System.out.println(SimilarityCalculation.SOUNDEX.getResult("bugger", "bag"));
    }
}
