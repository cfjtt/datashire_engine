package com.eurlanda.datashire.engine.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Created by zhudebin on 14-6-12.
 */
public class DateUtil {

    private static Logger logger = LoggerFactory.getLogger(DateUtil.class);

    private static ThreadLocal<SimpleDateFormat> tl1 = new ThreadLocal<SimpleDateFormat>() {
        @Override protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        }
    };
    private static ThreadLocal<SimpleDateFormat> tl2 = new ThreadLocal<SimpleDateFormat>() {
        @Override protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("HH:mm:ss");
        }
    };
    private static ThreadLocal<SimpleDateFormat> tl3 = new ThreadLocal<SimpleDateFormat>() {
        @Override protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yyyy-MM-dd");
        }
    };
    private static ThreadLocal<SimpleDateFormat> tl4 = new ThreadLocal<SimpleDateFormat>() {
        @Override protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yy/dd/MM HH:mm:ss");
        }
    };
    private static ThreadLocal<SimpleDateFormat> tl5 = new ThreadLocal<SimpleDateFormat>() {
        @Override protected SimpleDateFormat initialValue() {
            return new SimpleDateFormat("yy/dd/MM");
        }
    };

    static {
        logger.info("初始化系统参数=== 时区{}", ConfigurationUtil.SYSTEM_TIME_ZONE());
        TimeZone.setDefault(TimeZone.getTimeZone(ConfigurationUtil.SYSTEM_TIME_ZONE()));
    }

    /**
     * yyyy-MM-dd HH:mm:ss.SSS
     * @param source
     * @return
     * @throws ParseException
     */
    public Date parseByDefaultTimestampFormat(String source) throws ParseException {
        SimpleDateFormat obj = tl1.get();
        return obj.parse(source);
    }

    /**
     * yyyy-MM-dd HH:mm:ss.SSS
     * @param date
     * @return
     */
    public String formatByDefaultTimestampFormat(Date date) {
        SimpleDateFormat obj = tl1.get();
        return obj.format(date);
    }

    /**
     * HH:mm:ss
     * @param source
     * @return
     * @throws ParseException
     */
    public static Date parseByDefaultTimeFormat(String source) throws ParseException {
        SimpleDateFormat obj = tl2.get();
        return obj.parse(source);
    }

    /**
     * HH:mm:ss
     * @param date
     * @return
     */
    public static String formatByDefaultTimeFormat(Date date) {
        SimpleDateFormat obj = tl2.get();
        return obj.format(date);
    }

    /**
     * yyyy-MM-dd
     * @param source
     * @return
     * @throws ParseException
     */
    public static Date parseByDefaultDateFormat(String source) throws ParseException {
        SimpleDateFormat obj = tl3.get();
        return obj.parse(source);
    }

    /**
     * yyyy-MM-dd
     * @param date
     * @return
     */
    public static String formatByDefaultDateFormat(Date date) {
        SimpleDateFormat obj = tl3.get();
        return obj.format(date);
    }

    /**
     * yy/dd/MM HH:mm:ss
     * @param source
     * @return
     * @throws ParseException
     */
    public static Date parseByIidDateTimeFormat(String source) throws ParseException {
        SimpleDateFormat obj = tl4.get();
        return obj.parse(source);
    }

    /**
     * yy/dd/MM HH:mm:ss
     * @param date
     * @return
     */
    public static String formatByIidDateTimeFormat(Date date) {
        SimpleDateFormat obj = tl4.get();
        return obj.format(date);
    }

    /**
     * yy/dd/MM
     * @param source
     * @return
     * @throws ParseException
     */
    public static Date parseByIidDateFormat(String source) throws ParseException {
        SimpleDateFormat obj = tl5.get();
        return obj.parse(source);
    }

    /**
     * yy/dd/MM
     * @param date
     * @return
     */
    public static String formatByIidDateFormat(Date date) {
        SimpleDateFormat obj = tl5.get();
        return obj.format(date);
    }

    /**
     * 统一生成 SimpleDateFormat  生成的对象是线程不安全的,小心使用
     * @param pattern
     * @param locale
     * @return
     */
    public static SimpleDateFormat genSDF(String pattern, Locale locale) {
        return new SimpleDateFormat(pattern, locale);
    }

    /**
     * 用pattern解析source
     * @param pattern
     * @param source
     * @return
     * @throws ParseException
     */
    public static Date parse(String pattern, String source) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.parse(source);
    }

    /**
     * 用pattern解析source
     * @param pattern
     * @param locale
     * @param source
     * @return
     * @throws ParseException
     */
    public static Date parse(String pattern, Locale locale, String source) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern, locale);
        return sdf.parse(source);
    }

    /**
     * 用pattern 格式化date
     * @param pattern
     * @param date
     * @return
     */
    public static String format(String pattern, Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.format(date);
    }

    /**
     * 用pattern 格式化date
     * @param pattern
     * @param date
     * @return
     */
    public static String format(String pattern, Object date) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.format(date);
    }

    /**
     * 用pattern 格式化date
     * @param pattern
     * @param locale
     * @param date
     * @return
     */
    public static String format(String pattern, Locale locale, Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern, locale);
        return sdf.format(date);
    }

    public static Date sql2Timestamp(java.sql.Timestamp timestamp) {
        return timestamp ==null ? null : new Date(timestamp.getTime());
    }
    public static java.sql.Time util2Time(java.util.Date date) {
        return date ==null ? null : new java.sql.Time(date.getTime());
    }
    public static java.sql.Timestamp util2Timestamp(java.util.Date date) {
        return date ==null ? null : new java.sql.Timestamp(date.getTime());
    }

    /**
     * 当前时间 timestamp
     * @return
     */
    public static Timestamp nowTimestamp() {
        return new java.sql.Timestamp(System.currentTimeMillis());
    }

    public static Date sql2util(java.sql.Date date) {
        return date ==null ? null : new Date(date.getTime());
    }

    public static java.sql.Date util2sql(Date date) {
        return date ==null ? null : new java.sql.Date(date.getTime());
    }

    public static java.sql.Date long2sql(long date) {
        return new java.sql.Date(date);
    }
}
