package com.eurlanda.datashire.engine.util;

import com.eurlanda.datashire.engine.entity.TDataType;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;

import java.math.BigDecimal;

/**
 * 验证数字类型长度类
 * Created by Akachi on 2014/12/24.
 */
public class ImplicitInstalledUtil {
    /**
     * akachi 隠试转换
     * @param from
     * @param to
     * @param value
     * @param precision
     * @param scale
     * @param <T> 输出为java.long.Number
     * @return
     */
    public static <T>T implicitInstalled(TDataType from,SystemDatatype to,Object value,Integer precision,Integer scale){
//        if(from.equals(TDataType.sysType2TDataType(to.value()))&&to.value()!=SystemDatatype.DECIMAL.value() ){//相同不处理
//            return (T)value;
//        }
        if(value instanceof Double){//解决bug double 无法转换
            value = new BigDecimal((Double)value);
        }if(value instanceof Float){
            value = new BigDecimal((Float)value);
        }
        String str = value.toString();
        if(str.trim().equals("")){
            throw new IllegalArgumentException("空字符串不能转换为"+to+"类型");
        }
        switch (to) {
            case TINYINT://>-128 <127
                str=str.split("\\.")[0];
                if(Long.parseLong(str)<-128||Long.parseLong(str)>127){
                    throw new NumberFormatException("无法转换\""+str+"\"为TINYINT；数值必须>-128 <127");
                }
                return (T) new Integer(Integer.parseInt(str));
            case INT://>-2147483648 <2147483647
                str=str.split("\\.")[0];
                if(Long.parseLong(str)<-2147483648||Long.parseLong(str)>2147483647){
                    throw new NumberFormatException("无法转换\""+str+"\"为INT；数值必须>-2147483648 <2147483647");
                }
                return (T) new Integer(Integer.parseInt(str));
            case BIGINT://>-9223372036854775808 <9223372036854775807
                str=str.split("\\.")[0];
                Long l=0l;
                try {
                    l = new Long(Long.parseLong(str));
                }catch (NumberFormatException e){
                    throw new NumberFormatException("无法转换\""+str+"\"为INT；数值必须>-9223372036854775808 <9223372036854775807");
                }
                return (T) l;
            case BIT://=0 or =1
//                if(!str.equals("0")&&!str.equals("1")&&
//                        !str.toUpperCase().equals("Y")&&!str.toUpperCase().equals("N")&&
//                        !str.toUpperCase().equals("TRUE")&&!str.toUpperCase().equals("FALSE")){
//                    throw new NumberFormatException("无法转换\""+str+"\"为BIT；数值必须 =0 or =1");
//                }
                if("0".equalsIgnoreCase(str)||"N".equalsIgnoreCase(str)||"FALSE".equalsIgnoreCase(str)){
                    Boolean bl = new Boolean(false);
                    return (T)bl;
                }else if("1".equalsIgnoreCase(str)||"Y".equalsIgnoreCase(str)||"TRUE".equalsIgnoreCase(str)){
                    Boolean bl = new Boolean(true);
                    return (T)bl;
                }
                    throw new NumberFormatException("无法转换\""+str+"\"为BIT；数值必须 =0 or =1 or true or false or n or y");
//                return (T) new Boolean(str.equals("0"));
            case DECIMAL://> -10^38 +1 <10^38 -1
                try {
                    BigDecimal bd = new BigDecimal(str);
                    if (precision != null && scale != null) {
                        if (bd.scale() != scale) {
                            bd = MathUtil.formatBigDecimal(bd, scale);
                        }
                        if (bd.subtract(MathUtil.maxDecimal(precision, 0)).signum() > 0) {
                            throw new NumberFormatException("无法转换\"" + str + "\"为此DECIMAL；因为长度超过" + precision);
                        }
                    }
                    return (T) bd;
                }catch (java.lang.NumberFormatException e){
                 //   throw new NumberFormatException("无法转换或格式化" + str + "为此DECIMAL类型" );
                    throw new NumberFormatException(e.getMessage());
                }
            case FLOAT:
                try {
                    return (T) new Float(Float.parseFloat(str));
                }catch ( Exception e){
                    throw new NumberFormatException("无法转换或格式化" + str + "为此DOUBLE类型" );
                }
            case DOUBLE:
                try {
                    return (T) new Double(Double.parseDouble(str));
                }catch ( Exception e){
                    throw new NumberFormatException("无法转换或格式化" + str + "为此DOUBLE类型" );
                }
            case SMALLINT://  >-32768 <32767
                try {
                    str = str.split("\\.")[0];
                    if (Integer.parseInt(str) < -32768 || Integer.parseInt(str) > 32767) {
                        throw new NumberFormatException("无法转换\"" + str + "\"为SMALLINT；数值必须>-32768 <32767");
                    }
                    return (T) new Short(Short.parseShort(str));
                }catch (Exception e){
                    throw new NumberFormatException("无法转换或格式化" + str + "为此SMALLINT类型" );
                }

            default:
                break;
        }
        if(str==null){
            return null;
        }
        return (T)str;
    }
}
