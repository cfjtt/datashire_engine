package com.eurlanda.datashire.engine.enumeration;

import com.eurlanda.datashire.engine.util.TransformationTypeAdaptor;

import java.sql.Timestamp;

/**
 * 日期差单位
 * Created by Juntao.Zhang on 2014/5/15.
 */
public enum DifferenceType {
    YEAR(0),//年
    QUARTER(1),//季度
    MONTH(2),//月
    WEEK(3),//星期
    DAY(4),//天
    HOUR(5),//时
    MINUTE(6),//分
    SECOND(7);//秒

    private int value;

    DifferenceType(int value) {
        this.value = value;
    }

    public static DifferenceType parse(int value) {
        for (DifferenceType type : DifferenceType.values()) {
            if (type.getValue() - value == 0) {
                return type;
            }
        }
        return null;
    }

    public static long getTimeDateDiff(long a, long b, DifferenceType type) {
        long differ = 0;
        switch (type) {
            case YEAR:
                differ = (a - b) / (60 * 60 * 24 * 365 * 1000l);
                break;
            case QUARTER:
                differ = (a - b) / (60 * 60 * 24 * 90 * 1000l);
                break;
            case MONTH:
                differ = (a - b) / (60 * 60 * 24 * 30 * 1000l);
                break;
            case WEEK:
                differ = (a - b) / (60 * 60 * 24 * 7 * 1000l);
                break;
            case DAY:
                differ = (a - b) / (60 * 60 * 24 * 1000l);
                break;
            case HOUR:
                differ = (a - b) / (60 * 60 * 1000l);
                break;
            case MINUTE:
                differ = (a - b) / (60 * 1000l);
                break;
            case SECOND:
                differ = (a - b) / 1000l;
                break;
        }
        //  return new Double(Math.ceil(Math.abs(differ))).longValue();
        return new Double(Math.floor(differ)).longValue(); // SQL中有正负之分
    }

    /**
     *
     * @param fromTimestamp
     * @param toTimestamp
     * @param diffType
     * @return
     */
    public static long getTimeDateDiff(Timestamp fromTimestamp, Timestamp toTimestamp, DifferenceType diffType) {
        if(fromTimestamp.equals(toTimestamp)){
           return 0;
        }
        double res = 0;
        int sign = 1 ; // 哪个时间大
        if(fromTimestamp.getTime() > toTimestamp.getTime()) {
            Timestamp tmp = fromTimestamp;
            fromTimestamp = toTimestamp;
            toTimestamp = tmp;
            sign =- 1;
        }
        int yearAbsDiff =  toTimestamp.getYear() - fromTimestamp.getYear();
        int mothAbsDiff = toTimestamp.getMonth() - fromTimestamp.getMonth();
        long diffHaomiao = toTimestamp.getTime() - fromTimestamp.getTime();
      //   long dayAbsDiff = (long) (Math.ceil(diffHaomiao)/ (1000 * 3600* 24.0));
       // if(sign>0){
       //     dayAbsDiff += 1;
       //  }
        switch (diffType) {
            case YEAR:
                res = yearAbsDiff;
                break;
            case QUARTER:
                res = yearAbsDiff * 4 +  getQuarter(toTimestamp.getMonth()+1) - getQuarter(fromTimestamp.getMonth()+1);
                break;
            case MONTH:
                res= yearAbsDiff * 12 + mothAbsDiff;
                break;
            case WEEK:
               /* res = (long)Math.ceil(dayAbsDiff/7.0)-1;
                if(fromTimestamp.getDay() > fromTimestamp.getDay()) {
                    res -= 1;
                }*/
                Long[] shangyushuweek = TransformationTypeAdaptor.shangyushu(diffHaomiao,1000*3600*24*7L);
                res = shangyushuweek[0];
                break;
            case DAY:
               //  res = dayAbsDiff ;
                Long[] shangyushu = TransformationTypeAdaptor.shangyushu(diffHaomiao,1000*3600*24L);
                res = shangyushu[0];
                break;
            case HOUR:
                Long[] shangyushu2 = TransformationTypeAdaptor.shangyushu(diffHaomiao,1000*3600L);
                res = shangyushu2[0];
                break;
            case MINUTE:
                res = (long) (Math.ceil(diffHaomiao)/(1000 *60));
                break;
            case SECOND:
                res = (long) (Math.ceil(diffHaomiao)/1000);
                break;
        }
        return (long)res*sign;
    }

    private static int getQuarter(int month) {
        switch (month){
            case 1:
            case 2:
            case 3:
                return 1;
            case 4:
            case 5:
            case 6:
                return 2;
            case 7:
            case 8:
            case 9:
                return 3;
            case 10:
            case 11:
            case 12:
                return 4;
        }
        return 0;
    }

    public static void main(String[] args) {
        System.out.println(new Double(Math.ceil(-9.323)).longValue());
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
