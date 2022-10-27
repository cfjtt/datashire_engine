package com.eurlanda.datashire.engine.translation;

import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;

/**
 * Created by zhudebin on 14-4-21.
 */
public class ColumKeyConvertor {

    public static String genColumnKey(Transformation transformation) {
        if(transformation.getTranstype() == TransformationTypeEnum.VIRTUAL.value()) {
            return transformation.getId()+"";
        } else {
            return "";
        }

    }
}
