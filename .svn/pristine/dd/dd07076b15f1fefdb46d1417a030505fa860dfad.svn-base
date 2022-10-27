package com.eurlanda.datashire.engine.entity.transformation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Created by Akachi on 2015/1/12.
 */
public class Area {
    private String key;
    private String name;
    private String fullName;
    private String parentKey;
    private Integer level;

    private Map<String,Area> childs;//id,area

    /**
     * 通过文件初始化
     * @author Akachi
     * @E-Mail zsts@hotmail.com
     * @param str
     */
    public Area(String[] str){
        key = str[0];
        parentKey = str[1];
        name = str[5];
        level = Integer.parseInt(str[4]);
        fullName = str[7];
    }

    public void setLevel(Integer level) {
        this.level = level;
    }

    public Integer getLevel() {
        return level;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    public void setParentKey(String parentKey) {
        this.parentKey = parentKey;
    }

    public void setChilds(Map<String, Area> childs) {
        this.childs = childs;
    }

    public String getKey() {
        return key;
    }

    public String getName() {
        return name;
    }

    public String getFullName() {
        return fullName;
    }

    public String getParentKey() {
        return parentKey;
    }

    public Map<String, Area> getChilds() {
        return childs;
    }



    public Area getChild(String name) {
        for(Entry<String,Area> entry:childs.entrySet()){
            if(name.equals(entry.getValue().name)){
                return entry.getValue();
            }
        }
        return null;
    }

    public Area getChild(String name,boolean isFull) {
        for(Entry<String,Area> entry:childs.entrySet()){
            if(name.equals(entry.getValue().fullName)){
                return entry.getValue();
            }
        }
        return null;
    }
    public List<String> getChildsStr() {
        List<String> list = new ArrayList<String>();
        for(Entry<String,Area> entry:childs.entrySet()){
            if(entry.getValue()==null||entry.getValue().getName()==null){
                continue;
            }
            list.add(entry.getValue().getName());
        }
        return list;
    }
    public List<String> getChildsStr(List<String> list,Map<String,Area> childs) {
        for(Entry<String,Area> entry:childs.entrySet()){
            if(entry.getValue()==null||entry.getValue().getName()==null){
                continue;
            }
            list.add(entry.getValue().getName());
        }
        return list;
    }
    public List<String> getCityStr(){
        List<String> list = new ArrayList<String>();
        for(Entry<String,Area> entry:childs.entrySet()){
            if(entry.getValue()==null||entry.getValue().getName()==null || entry.getValue().getChilds() == null){
                continue;
            }
            entry.getValue().getChildsStr(list,entry.getValue().getChilds());
            //list.add(entry.getValue().getName());
        }
        return list;
    }
    public List<String> getChildsStr(boolean isFull) {
        List<String> list = new ArrayList<String>();
        for(Entry<String,Area> entry:childs.entrySet()){
            if(entry.getValue()==null||entry.getValue().getFullName()==null){
                continue;
            }
            list.add(entry.getValue().getFullName());
        }
        return list;
    }
    /*public List<String> getChildsStr(Integer level){
        List<String> list = new ArrayList<>();
        for(Entry<String,Area> entry:childs.entrySet()){

        }
    }*/
}
