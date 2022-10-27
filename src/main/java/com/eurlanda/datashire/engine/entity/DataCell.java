package com.eurlanda.datashire.engine.entity;

import com.eurlanda.datashire.engine.util.ListUtil;
import com.eurlanda.datashire.engine.util.MapUtil;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * Created by zhudebin on 13-12-31.
 */
public class DataCell implements Serializable {

    private static Logger logger = Logger.getLogger(DataCell.class);

    private static final long serialVersionUID = 1L;

    private TDataType dType;
    private Object data;

    public DataCell() {
    }

    public DataCell(TDataType dType, Object data) {
        this.dType = dType;
        this.data = data;
    }

    public TDataType getdType() {
        return dType;
    }

    public void setdType(TDataType dType) {
        this.dType = dType;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
    	if(data instanceof java.lang.Double){
			if (((Double) data).isNaN()) {
				//throw new RuntimeException("数值不合法，输入值：[" + data + "] ");
				data=null;
			}
    	}else if(data instanceof java.lang.Float){
    		if (((Float) data).isNaN()) {
				//throw new RuntimeException("数值不合法，输入值：[" + data + "] ");
    			data=null;
			}
    	}
        this.data = data;
    }

    /**
     * 数据元相加
     *
     * @param dc
     * @return
     */
    public void add(DataCell dc) {
        if (dc != null) {
            // TODO 对于数据相加溢出暂时不考虑，等待元数据类型数据
            if (dc.data != null) {
                switch (dType) {
                    case BIG_DECIMAL:
                        this.data = ((BigDecimal) this.data).add((BigDecimal) dc.data);
                        break;
                    case TINYINT:
                        this.data = (byte)((byte) this.data + (byte) dc.data);
                        break;
                    case SHORT:
                        this.data = (short) this.data + (short) dc.data;
                        break;
                    case INT:
                        this.data = (int) this.data + (int) dc.data;
                        break;
                    case LONG:
                        this.data = (long) this.data + (long) dc.data;
                        break;
                    case FLOAT:
                        this.data = (float) this.data + (float) dc.data;
                        break;
                    case DOUBLE:
                        this.data = (double) this.data + (double) dc.data;
                        break;
                    case STRING:
                        this.data = this.data + (String) dc.data;
                        break;
                    case CSV:
                        this.data= this.data.toString()+","+dc.data.toString();
                        break;
                    default:
                        throw new RuntimeException("该类型DATACELL[" + dType.name() + "]不能执行add操作");
                }
            }
        } else {
            logger.error("DataCell add() 异常：相加dc为null...");
        }
    }

    /**
     * 求两个的最大值
     *
     * @param dc
     * @return
     */
    public void max(DataCell dc) {
//        if (this.compare(dc) > 0) {
//
//        } else {
//            this.data = dc.data;
//        }

        if (this.compare(dc) <= 0) {
            this.data = dc.data;
        }
    }

    /**
     * 求两个的最小值
     *
     * @param dc
     * @return
     */
    public void min(DataCell dc) {
//        if (this.compare(dc) < 0) {
//        } else {
//            this.data = dc.data;
//        }

        if (this.compare(dc) >= 0) {
            this.data = dc.data;
        }
    }

    public int compareNoNull(DataCell dc) {
        if (dc.dType == this.dType) {
            switch (dType) {
                case VARBINARY:
                    return (int)(((byte[]) this.data).length - ((byte[]) dc.data).length);
                case BIG_DECIMAL:
                    return (int)(((BigDecimal) this.data).subtract((BigDecimal) dc.data).doubleValue());
                case TIMESTAMP:
                    return (int)(((java.sql.Timestamp) this.data).getTime() - ((java.sql.Timestamp) dc.data).getTime());
                case DATE:
                    return (int)(((java.sql.Date) this.data).getTime() - ((java.sql.Date) dc.data).getTime());
//                case TIME:
//                    return (int)(((java.sql.Time) this.data).getTime() - ((java.sql.Time) dc.data).getTime());
                case INT:
                    return (int) this.data - (int) dc.data;
                case LONG:
                    return (int)((long) this.data - (long) dc.data);
                case FLOAT:
                case DOUBLE:
                    double tmp = (double) this.data - (double) dc.data;
                    return tmp == 0 ? 0 : (tmp > 0 ? 1 : -1);
                case STRING:
                case CSN:
                case CSV:
                    return ((String) this.data).compareTo((String) dc.data);
                default:
                    throw new RuntimeException("不能对" + dType + " 该数据类型进行比较操作");
            }
        } else {
            throw new RuntimeException("DataCell 比较异常：类型不匹配...");
        }
    }

    private int compare(DataCell dc) {
        if (dc != null) {
            if (dc.dType == this.dType) {
                if (dc.data != null) {
                    switch (dType) {
                        case VARBINARY:
                            if (((byte[]) this.data).length > ((byte[]) dc.data).length) {
                                return 1;
                            } else {
                                return -1;
                            }
                        case BIG_DECIMAL:
                            if (((BigDecimal) this.data).subtract((BigDecimal) dc.data).doubleValue() > 0) {
                                return 1;
                            } else {
                                return -1;
                            }
                        case TIMESTAMP:
                            if (((java.sql.Timestamp) this.data).getTime() > ((java.sql.Timestamp) dc.data).getTime()) {
                                return 1;
                            } else {
                                return -1;
                            }
                        case DATE:
                            if (((java.sql.Date) this.data).getTime() > ((java.sql.Date) dc.data).getTime()) {
                                return 1;
                            } else {
                                return -1;
                            }
//                        case TIME:
//                            if (((java.sql.Time) this.data).getTime() > ((java.sql.Time) dc.data).getTime()) {
//                                return 1;
//                            } else {
//                                return -1;
//                            }
                        case TINYINT:
                            if ((Byte) this.data > (Byte) dc.data) {
                                return 1;
                            } else {
                                return -1;
                            }
                        case SHORT:
                        case INT:
                            if ((int) this.data > (int) dc.data) {
                                return 1;
                            } else {
                                return -1;
                            }
                        case LONG:
                            if ((long) this.data > (long) dc.data) {
                                return 1;
                            } else {
                                return -1;
                            }
                        case FLOAT:
                            if ((Float) this.data > (Float) dc.data) {
                                return 1;
                            } else {
                                return -1;
                            }
                        case DOUBLE:
                            if ((double) this.data > (double) dc.data) {
                                return 1;
                            } else {
                                return -1;
                            }
                        case STRING:
                            return ((String) this.data).compareTo((String) dc.data);
                        default:
                            throw new RuntimeException("该类型DATACELL[" + dType.name() + "]不能执行compare操作");
                    }
                }
            } else {
                logger.error("DataCell compare() 异常：类型不匹配...");
            }
        } else {
            logger.error("DataCell compare() 异常：comparedc为null...");
        }
        return 1;
    }

    @Override
    public DataCell clone() {
        return new DataCell(this.dType, this.data);
    }

    @Override
    public String toString() {
        return "[type:" + dType + ",data:" + data + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DataCell)) return false;

        DataCell dataCell = (DataCell) o;

        if (dType != dataCell.dType) return false;
        if(data == null) {
            if(o == null || dataCell.getData() == null) {
                return true;
            } else {
                return false;
            }
            // todo
        } else {
            if(dataCell == null || dataCell.getData() == null) {
                return false;
            }
            if(data instanceof Integer) {
                if(((Integer)data).intValue() != ((Integer)dataCell.data).intValue()) {
                    return false;
                }
            } else if(data instanceof Double) {
                if(((Double)data).doubleValue() != ((Double)dataCell.data).doubleValue()) {
                    return false;
                }
            } else if(data instanceof Float) {
                if(((Float)data).floatValue() != ((Float)dataCell.data).floatValue()) {
                    return false;
                }
            } else if(data instanceof Character) {
                if(((Character)data).charValue() != ((Character)dataCell.data).charValue()) {
                    return false;
                }
            } else {
                if (!data.equals(dataCell.data)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
    	
        //不同的jvm TDataType enum 的hashCode 是不同的
        int result = dType.getDataType();
        result = (31 * result) + (data==null?0:data.hashCode());
        return result;
    }

    public String printString() {
        if(data == null) {
            return "null";
        } else {
            switch (dType) {

            case DATE:
                return new DateTime(((Date)data).getTime()).toString("yyyy-MM-dd HH:mm:ss.SSS");
//            case TIME:
//                return new DateTime(((Time)data).getTime()).toString("yyyy-MM-dd HH:mm:ss.SSS");
            case TIMESTAMP:
                return new DateTime(((Timestamp)data).getTime()).toString("yyyy-MM-dd HH:mm:ss.SSS");
            case ARRAY:
                List list  = (List)data;
                return ListUtil.mkString(list, "[", ",", "]");
            case MAP:
                return MapUtil.toString((Map)data);
            case VARBINARY:
                return ListUtil.mkString((byte[])data, "[", ",", "]");

            // --------- toString ---------
            // 引擎运行中使用的数据格式  ---- start  ---
            case LABELED_POINT:
            case RATING:
            // 引擎运行中使用的数据格式  ---- end ---
            case CSN:
            case STRING:
            case INT:
            case LONG:
            case DOUBLE:
            case FLOAT:
            case BIG_DECIMAL:
            case BOOLEAN:
            case CSV:
            default:
                return data.toString();
            }
        }
    }

}
