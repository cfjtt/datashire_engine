package com.eurlanda.datashire.engine.entity.transformation;

import java.util.List;
import java.util.Map;

/**
 * Created by Akachi on 2015/1/13.
 */
public class AreaProxy {
    private Area theArea;

    public Area getTheArea() {
        return theArea;
    }

    public void setTheArea(Area theArea) {
        this.theArea = theArea;
    }

    /**
     * 通过文件初始化
     * @author Akachi
     * @E-Mail zsts@hotmail.com
     * @param str
     */
    public AreaProxy(String[] str){
        theArea = new Area(str);
    }
    public AreaProxy(Area theArea){
        this.theArea = theArea;
    }



    public void setLevel(Integer level) {
        theArea.setLevel(level);
    }

    public Integer getLevel() {
        return theArea.getLevel();
    }

    public void setKey(String key) {
        theArea.setKey( key);
    }

    public void setName(String name) {
        theArea.setName(name);
    }

    public void setFullName(String fullName) {
        theArea.setFullName(fullName);
    }

    public void setParentKey(String parentKey) {
        theArea.setParentKey(parentKey);
    }

    public void setChilds(Map<String, Area> childs) {
        theArea.setChilds(childs);
    }

    public String getKey() {
        return theArea.getKey();
    }

    public String getName() {
        return theArea.getName();
    }

    public String getFullName() {
        return theArea.getFullName();
    }

    public String getParentKey() {
        return theArea.getParentKey();
    }

    public Map<String, Area> getChilds() {
        return theArea.getChilds();
    }



    public Area getChild(String name) {
        return theArea.getChild(name);
    }

    public Area getChild(String name,boolean isFull) {
        return theArea.getChild(name,isFull);
    }
    public List<String> getChildsStr() {
        return theArea.getChildsStr();
    }

    public List<String> getCityStr(){
        return theArea.getCityStr();
    }
    public List<String> getChildsStr(boolean isFull) {
        return theArea.getChildsStr(isFull);
    }

    /*public List<String> getChildStr(Integer level){
        return theArea.getChildsStr(level);
    }*/
}
