package com.eurlanda.datashire.engine.util;

import com.eurlanda.datashire.engine.entity.DataCell;
import com.eurlanda.datashire.engine.entity.TOrderItem;

import java.util.List;
import java.util.Map;

/**
 * Created by zhudebin on 15-4-17.
 */
public class OrderUtil {

    /**
     * 通过orders比较两行数据
     * @param first
     * @param last
     * @param orders
     * @return first>last 返回1, first=last 返回 0， fist<last 返回 -1
     */
    public static int compare(Map<Integer, DataCell> first, Map<Integer, DataCell> last, List<TOrderItem> orders) {
        int result = 0;
        for(TOrderItem oi : orders) {
            int key = oi.getKey();
            boolean ascending = oi.isAscending();
            int dcRsult = compareDatacell(first.get(key), last.get(key));
            if(dcRsult == 0 ) {
                continue;
            }
            if(dcRsult > 0) {
                return ascending?1:-1;
            }
            if(dcRsult < 0) {
                return ascending?-1:1;
            }
        }

        return result;
    }

    /**
     * null < notNull
     * @param dc1
     * @param dc2
     * @return dc1>dc2 return 1;dc1=dc2 return 0; dc1<dc2 return -1;
     */
    private static int compareDatacell(DataCell dc1, DataCell dc2) {
        if(isNull(dc1)) {
            if(isNull(dc2)) {
                return 0;
            } else {
                return -1;
            }
        } else if(isNull(dc2)) {
            return 1;
        } else {
            return dc1.compareNoNull(dc2);
        }
    }

    private static boolean isNull(DataCell dc) {
        return dc == null || dc.getData() == null;
    }

    private static int compareObj(Object o1, Object o2) {
        return 0;
    }
}
