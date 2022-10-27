package com.eurlanda.datashire.engine.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * Created by Akachi on 2015/1/8.
 */
public class DictionaryMatchUtil {
    public static final Integer MOD_FIRST=1;
    public static final Integer MOD_LAST=2;
    public static final Integer MOD_LEFT=3;
    public static final Integer MOD_RIGHT=4;

    private List<String> wordbook;

    /**
     * 格式化字符串，去掉startIndex左边的字符串，找到存在于sourceStr中的wordbook.item。
     * 根据模式返回结果，1=第一个匹配到的，2=最后一个匹配到的，left=最左边一个匹配到的。right=最右边一个匹配到的。
     * @author Akachi
     * @E_Mail zsts@hotmail.com
     * @param startIndex 字符串开始
     * @param sourceStr 源字符串
     * @param mod 模式1=第一个，2=最后一个，3=最左边，4=最右边
     * @param isFull 1 返回匹配的长的item,2 返回匹配的短的item匹配,3 返回所有匹配的item,
     * @return
     */
    public List<String> match(Integer startIndex,String sourceStr,Integer mod,Integer isFull){
        sourceStr = sourceStr.substring(startIndex);
        Map<Integer,String> returnMap=new HashMap<Integer,String>();
        List<String> returnStr = new ArrayList<String>();
        if(mod==3){
            Integer leftIndex = sourceStr.length()-1;
            for(int i = wordbook.size();i>0;i--){
                String wordbookItem = wordbook.get(i-1);
                int index = sourceStr.indexOf(wordbookItem);
                if(index!=-1){
                    if(index==leftIndex//靠左下标相同
                            &&returnMap.get(leftIndex)!=null&&
                            (isFull==1&&returnMap.get(leftIndex).length()<wordbookItem.length()//取全称
                                    ||isFull==2&&returnMap.get(leftIndex).length()>wordbookItem.length())){//取简称
                        leftIndex = index;
                        returnMap.put(index,wordbookItem);
                    }else if(isFull==3){//取得所有
                        returnStr.add(wordbookItem);
                    }else if(index!=leftIndex){
                        leftIndex = index;
                        returnMap.put(index,wordbookItem);
                    }
                }
            }
            if((isFull==1||isFull==2)&&returnMap.get(leftIndex)!=null) {
                returnStr.add(returnMap.get(leftIndex));
            }
        }else if(mod==4){
            Integer rightIndex = 0;
            for(int i = 0;i<wordbook.size();i++){
                String wordbookItem = wordbook.get(i);
                int index = sourceStr.indexOf(wordbookItem);
                if(index!=-1){
                    if(index+wordbookItem.length()>=rightIndex){
                        if(rightIndex==index+wordbookItem.length()//靠右下标相同
                                &&returnMap.get(rightIndex)!=null
                                &&(isFull==1&&returnMap.get(rightIndex).length()<wordbookItem.length()//取全称
                                    ||isFull==2&&returnMap.get(rightIndex).length()>wordbookItem.length())){//取简称
                            rightIndex = index;
                            returnMap.put(index,wordbookItem);
                        }else if(isFull==3){//取得所有
                            returnStr.add(wordbookItem);
                        }else if(rightIndex!=index+wordbookItem.length()){
                            rightIndex = index;
                            returnMap.put(index,wordbookItem);
                        }
                    }
                }
            }
            if((isFull==1||isFull==2)&&returnMap.get(rightIndex)!=null) {
                returnStr.add(returnMap.get(rightIndex));
            }
        }else if(mod==1){//first
            for(int i = 0;i<wordbook.size();i++){
                String wordbookItem = wordbook.get(i);
                int index = sourceStr.indexOf(wordbookItem);
                if(index!=-1){
                    returnStr.add(wordbookItem);
                    break;
                }
            }
        }else if(mod==2){//last
            for(int i = 0;i<wordbook.size();i++){
                String wordbookItem = wordbook.get(i);
                int index = sourceStr.indexOf(wordbookItem);
                if(index!=-1){
                    returnStr.add(wordbookItem);
                }
            }
        }
        return returnStr;
    }

    public List<String> getWordbook() {
        return wordbook;
    }

    public void setWordbook(List<String> wordbook) {
        this.wordbook = wordbook;
    }


}
